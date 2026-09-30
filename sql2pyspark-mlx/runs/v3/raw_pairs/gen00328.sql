select id, brand, explode(tags) as label from products;
