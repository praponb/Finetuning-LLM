SELECT QUARTER(p.launch_date) AS launch_quarter, MAX(i.quantity) - MIN(i.quantity) AS quantity_range, MAX(i.last_restock) AS last_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
GROUP BY QUARTER(p.launch_date)
ORDER BY launch_quarter
LIMIT 5
