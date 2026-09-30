SELECT category, supplier_id, CONCAT(brand, ' ', category) AS brand_category
FROM products
WHERE price BETWEEN 980 AND 1190;
