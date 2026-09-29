select id, category, brand, explode(tags) as tag from products where category not in ('Sports', 'Electronics', 'Grocery');
