SELECT product_id, quantity, store_id, ROUND(quantity * 1.1, 2) AS quantity_scaled FROM inventory WHERE store_id = 3 AND quantity <> 73 ORDER BY product_id DESC
