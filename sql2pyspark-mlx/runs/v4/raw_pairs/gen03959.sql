SELECT QUARTER(s.opened_date) opened_quarter, SUM(i.quantity) total_quantity, MAX(i.last_restock) latest_last_restock
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
WHERE s.sqft BETWEEN 8528 AND 15220
GROUP BY QUARTER(s.opened_date)
ORDER BY 2;
