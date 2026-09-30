SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, COUNT(DISTINCT i.quantity) AS unique_stores, MAX(i.quantity) AS max_quantity
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
INNER JOIN stores s ON i.store_id = s.id
WHERE i.quantity > 233
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY max_quantity DESC, opened_month
LIMIT 10
