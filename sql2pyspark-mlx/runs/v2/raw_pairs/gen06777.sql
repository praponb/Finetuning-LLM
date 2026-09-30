SELECT product_id, MAX(quantity) - MIN(quantity) quantity_range, SUM(quantity) total_quantity
FROM inventory
GROUP BY 1;
