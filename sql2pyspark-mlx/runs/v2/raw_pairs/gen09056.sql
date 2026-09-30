SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') opened_month, AVG(i.quantity) avg_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.product_id IN (1, 2, 3, 8)
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY avg_quantity DESC
