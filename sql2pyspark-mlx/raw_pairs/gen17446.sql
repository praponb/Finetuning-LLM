SELECT *, ROUND(quantity, 2) AS quantity_rounded, CAST(quantity AS STRING) AS quantity_str
FROM inventory
WHERE product_id IN (4, 6);
