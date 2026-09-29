SELECT p.brand, YEAR(p.launch_date) AS launch_year, COUNT(ss.revenue) AS revenue_count
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
GROUP BY p.brand, YEAR(p.launch_date)
HAVING revenue_count > 5
ORDER BY brand, launch_year
LIMIT 10
