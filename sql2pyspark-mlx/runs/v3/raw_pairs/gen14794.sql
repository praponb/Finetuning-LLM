SELECT ss.product_id, MAX(i.quantity) - MIN(i.quantity) AS quantity_range, COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2023-10-01'
GROUP BY ss.product_id
HAVING quantity_count > 5
ORDER BY quantity_range DESC;
