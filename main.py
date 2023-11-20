from bs4 import BeautifulSoup
import requests
import concurrent.futures
import pandas as pd

def get_html(url):
    response = requests.get(url)
    return response.text

category_url = 'https://online.metro-cc.ru/category/bezalkogolnye-napitki/pityevaya-voda-kulery'
base_url = 'https://online.metro-cc.ru'
products_data = []
total_pages = 17



def parse_product_info(product_block):
    product_name = product_block.find('span', class_='product-card-name__text').text.strip()
    
    # actual
    actual_price_element = product_block.find('div', class_='product-unit-prices__actual-wrapper')
    act_product_price_element = actual_price_element.find('span', class_='product-price__sum-rubles')
    act_product_penny_element = actual_price_element.find('span', class_='product-price__sum-penny')
    act_product_price = act_product_price_element.text.strip() if act_product_price_element else None
    act_product_penny = act_product_penny_element.text.strip() if act_product_penny_element else ''

    # regular
    regular_price_element = product_block.find('div', class_='product-unit-prices__old-wrapper')
    reg_product_price_element = regular_price_element.find('span', class_='product-price__sum-rubles')
    reg_product_penny_element = regular_price_element.find('span', class_='product-price__sum-penny')
    reg_product_price = reg_product_price_element.text.strip() if reg_product_price_element else None
    reg_product_penny = reg_product_penny_element.text.strip() if reg_product_penny_element else ''

    if not reg_product_price :
        [reg_product_price, reg_product_penny] = [act_product_price, act_product_penny]
        act_product_penny = None
        act_product_price = ''

    
    product_link = product_block.find('a', class_='product-card-name', href=True)['href']
    resp = requests.get(f'{base_url}{product_link}')
    sp = BeautifulSoup(resp.content, 'html.parser')
    span_el = sp.find('span', class_='product-attributes__list-item-value')
    product_brand = span_el.text.strip() if span_el else None

   


    return {
        'Название': product_name,
        'Регулярная цена': f'{reg_product_price}{reg_product_penny}',
        'Промо цена': f'{act_product_price}{act_product_penny}',
        'Ссылка': f'{base_url}{product_link}',
        'id': product_block['data-sku'],
        'Бренд': product_brand
        }


def parse_metro_category(category_url, total_pages):
    all_products_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        page_urls = [f'{category_url}?from=under_search&page={page}' for page in range(1, total_pages + 1)]
        results = executor.map(parse_metro_category_page, page_urls)
    for result in results:
        all_products_data.extend(result)
    return all_products_data


def parse_metro_category_page(page_url):
    html = get_html(page_url)
    soup = BeautifulSoup(html, 'html.parser')
    product_blocks = soup.find_all('div', class_='catalog-2-level-product-card product-card subcategory-or-type__products-item with-rating with-prices-drop')

    products_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(parse_product_info, product_blocks)
    for result in results:
        products_data.append(result)

    return products_data

products_data = parse_metro_category(category_url, total_pages)

df = pd.DataFrame(products_data)
df.to_csv('metro_products.csv', index=True)