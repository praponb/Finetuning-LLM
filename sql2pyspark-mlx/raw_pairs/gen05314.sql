SELECT QUARTER(s.opened_date) AS opened_quarter, MAX(i.quantity) AS max_quantity, SUM(i.quantity) AS total_quantity
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
WHERE i.store_id IN (2, 4)
GROUP BY QUARTER(s.opened_date);
