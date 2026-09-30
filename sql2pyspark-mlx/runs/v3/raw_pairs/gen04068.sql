SELECT MONTH(s.opened_date) AS opened_month, MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON s.id = i.store_id
WHERE MONTH(s.opened_date) IN (1, 11, 12)
GROUP BY MONTH(s.opened_date);
