SELECT s.id, s.city, s.country
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE YEAR(launch_date) >= 2024)
ORDER BY s.id DESC
LIMIT 50;
