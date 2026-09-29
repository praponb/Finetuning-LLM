SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range FROM inventory WHERE product_id IN (2, 5, 8, 9) AND store_id IN (1, 2, 6) GROUP BY product_id;
