select id, product_name, price, explode(tags) as tag_name from products where brand = 'Initech'
