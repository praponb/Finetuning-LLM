SELECT supplier_id, brand, ARRAY_JOIN(tags, '|') AS tags_text
FROM products
WHERE product_name IS NOT NULL AND category IN ('Books', 'Grocery');
