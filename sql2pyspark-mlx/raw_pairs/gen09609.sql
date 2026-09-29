select id, price, category, explode(tags) as tag from products
