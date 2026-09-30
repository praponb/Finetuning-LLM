SELECT id, product_id, store_id, quantity, CAST(quantity AS STRING) AS quantity_str FROM inventory WHERE MONTH(last_restock) IN (2, 5, 9) AND store_id IN (2, 4, 5);
