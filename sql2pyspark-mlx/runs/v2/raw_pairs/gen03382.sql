SELECT id, price, CONCAT_WS('-', brand, category) AS brand_category_key
FROM products
WHERE price < 700 AND (ARRAY_CONTAINS(tags, 'new') OR price > 660)
