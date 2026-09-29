SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, s.city, COUNT(DISTINCT i.quantity) AS unique_products
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.quantity <= 136
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM'), s.city
ORDER BY city;
