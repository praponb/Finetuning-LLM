SELECT launch_date, brand, CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END AS is_electronics
FROM products
WHERE stock_qty BETWEEN 112 AND 144
ORDER BY launch_date ASC, is_electronics DESC
