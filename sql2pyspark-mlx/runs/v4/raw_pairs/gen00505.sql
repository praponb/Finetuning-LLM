select id, product_name, explode(tags) as label from products where price < 1090;
