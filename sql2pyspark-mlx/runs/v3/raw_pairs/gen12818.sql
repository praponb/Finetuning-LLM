SELECT s.city, MAX(p.launch_date) AS last_launch_date
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY s.city
ORDER BY city, last_launch_date
