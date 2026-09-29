select id, price, explode(tags) as tag from products where supplier_id in (11, 19, 36, 40);
