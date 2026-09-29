select id, brand, product_name, explode(tags) as tag_name from products where supplier_id = 33
