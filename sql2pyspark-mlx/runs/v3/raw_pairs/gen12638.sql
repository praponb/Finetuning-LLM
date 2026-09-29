select id, brand, category, explode(tags) as tag from products where brand != 'Globex'
