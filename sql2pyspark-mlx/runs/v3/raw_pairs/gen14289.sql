SELECT category, SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END) AS stark_count FROM products WHERE supplier_id = 33 GROUP BY 1 HAVING SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END) < 40;
