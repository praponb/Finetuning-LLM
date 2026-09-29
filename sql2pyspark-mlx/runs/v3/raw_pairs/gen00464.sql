SELECT product_id, store_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE last_restock <= '2025-08-08' AND quantity BETWEEN 94 AND 134
GROUP BY 1, 2;
