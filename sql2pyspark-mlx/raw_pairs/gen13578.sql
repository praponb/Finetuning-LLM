SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, MAX(i.quantity) - MIN(i.quantity) AS quantity_range, COUNT(*) AS row_count
FROM inventory i
LEFT OUTER JOIN products p ON p.id = i.product_id
LEFT OUTER JOIN stores s ON s.id = i.store_id
WHERE i.quantity < 219
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY quantity_range DESC
