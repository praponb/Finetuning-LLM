SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, COUNT(*) AS total_count, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
WHERE i.quantity BETWEEN 98 AND 104
GROUP BY DATE_TRUNC('MONTH', s.opened_date)
ORDER BY total_count, 3
