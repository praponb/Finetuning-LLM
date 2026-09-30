SELECT p.brand, MONTH(p.launch_date) AS launch_month, COUNT(DISTINCT ss.units) AS unique_products
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.product_id IN (3, 7)
GROUP BY p.brand, MONTH(p.launch_date)
HAVING COUNT(DISTINCT ss.units) > 1
