SELECT id, last_restock, ROUND(quantity, 0) AS quantity_rounded
FROM inventory
WHERE product_id IN (1, 2, 4) AND store_id IN (4, 5)
ORDER BY last_restock DESC;
