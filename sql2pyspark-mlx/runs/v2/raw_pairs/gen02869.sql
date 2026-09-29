select id, brand, price, explode(tags) as label from products;
