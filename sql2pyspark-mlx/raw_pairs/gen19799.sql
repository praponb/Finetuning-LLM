select id, brand, price, explode(tags) as tag_name from products
