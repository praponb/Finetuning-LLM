select id, brand, product_name, explode(tags) as tag from products where category in ('Sports', 'Fashion')
