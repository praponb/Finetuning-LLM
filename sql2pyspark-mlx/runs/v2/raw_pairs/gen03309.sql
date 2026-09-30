SELECT brand, cost, CONCAT_WS('-', category, brand) AS category_brand_key FROM products
