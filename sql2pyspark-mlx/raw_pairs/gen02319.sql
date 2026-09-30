SELECT *, CAST(quantity AS STRING) AS quantity_num, COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE quantity > 183
ORDER BY last_restock DESC;
