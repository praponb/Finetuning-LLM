select id, price, explode(tags) as tag from products;
