SELECT ROUND((100 * SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_stark FROM products WHERE product_name IS NOT NULL AND cost BETWEEN 490 AND 520 AND supplier_id IN (21, 35);
