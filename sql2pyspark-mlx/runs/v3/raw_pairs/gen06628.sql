SELECT QUARTER(s.opened_date) AS opened_quarter, MIN(i.last_restock) AS min_last_restock, COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
GROUP BY QUARTER(s.opened_date)
ORDER BY quantity_count, opened_quarter DESC;
