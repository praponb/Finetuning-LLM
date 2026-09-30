SELECT s.city, s.country, MAX(p.launch_date) AS latest_launch_date
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city, s.country
