SELECT COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count
FROM products
WHERE cost IS NOT NULL AND brand = 'Acme'
ORDER BY electronics_count
