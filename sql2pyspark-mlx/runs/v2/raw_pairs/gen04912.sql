select id, brand, explode(tags) as label from products where supplier_id is null
