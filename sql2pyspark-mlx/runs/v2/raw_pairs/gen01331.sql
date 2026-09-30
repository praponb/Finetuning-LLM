SELECT p.supplier_id, ROUND(AVG(ss.units), 2) AS avg_units
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
WHERE p.launch_date BETWEEN '2023-11-01' AND '2025-09-01'
GROUP BY p.supplier_id
ORDER BY supplier_id, avg_units DESC
LIMIT 10;
