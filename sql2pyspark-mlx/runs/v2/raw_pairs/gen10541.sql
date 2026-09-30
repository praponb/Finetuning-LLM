SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, MIN(i.quantity) AS min_quantity
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE i.store_id IN (2, 4, 5, 6)
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY opened_month DESC, 2 DESC
