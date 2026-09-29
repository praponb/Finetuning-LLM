SELECT product_id, MIN(quantity) AS min_quantity, COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY product_id
