select id, category, price, explode(tags) as tag from products;
