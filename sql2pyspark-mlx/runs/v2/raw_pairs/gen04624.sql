SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range FROM inventory WHERE NOT (quantity != 85 OR product_id IN (3, 8, 10)) GROUP BY store_id
