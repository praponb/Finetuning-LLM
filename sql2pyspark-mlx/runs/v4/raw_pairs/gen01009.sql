SELECT *, ROUND(quantity * 1.07, 2) AS quantity_adjusted, CAST(quantity AS STRING) AS quantity_str FROM inventory WHERE quantity BETWEEN 145 AND 186 ORDER BY quantity_adjusted, store_id DESC
