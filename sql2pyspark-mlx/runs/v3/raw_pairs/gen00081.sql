SELECT id, product_id, DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label, CAST(quantity AS STRING) AS quantity_str FROM inventory ORDER BY last_restock_label ASC
