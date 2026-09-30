select id, price, brand, explode(tags) as tag_name from products where supplier_id in (15, 33);
