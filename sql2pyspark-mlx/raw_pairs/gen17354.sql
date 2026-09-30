SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, AVG(i.quantity) AS avg_quantity, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
JOIN stores s ON s.id = i.store_id
RIGHT JOIN products p ON i.product_id = p.id
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
HAVING AVG(i.quantity) >= 80
