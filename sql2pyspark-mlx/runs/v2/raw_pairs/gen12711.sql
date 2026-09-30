SELECT ss.store_id, MIN(i.quantity) AS min_quantity, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock BETWEEN '2024-01-01' AND '2025-02-01'
GROUP BY ss.store_id
HAVING MIN(i.quantity) > 120;
