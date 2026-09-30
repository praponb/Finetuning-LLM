SELECT *, CONCAT(product_name, ' ', category) AS product_name_category, CASE WHEN category = 'Home' THEN 1 ELSE 0 END AS is_home FROM products WHERE supplier_id IN (9, 12, 14);
