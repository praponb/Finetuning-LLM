SELECT MONTH(s.opened_date) AS opened_month, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
GROUP BY MONTH(s.opened_date)
ORDER BY opened_month, 2 DESC
LIMIT 50
