SELECT s.city, COUNT(p.id) AS product_count, MAX(p.launch_date) AS latest_launch_date
FROM suppliers s
INNER JOIN products p ON p.supplier_id = s.id
GROUP BY s.city
ORDER BY product_count DESC;
