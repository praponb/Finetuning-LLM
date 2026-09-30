select id, price, brand, explode(tags) as tag from products
