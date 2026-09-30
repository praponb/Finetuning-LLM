select id, brand, category, explode(tags) as tag from products where supplier_id in (10, 19, 34)
