SELECT quantity, product_id, CAST(quantity AS STRING) AS quantity_str
FROM inventory
WHERE MONTH(last_restock) IN (6, 8, 11);
