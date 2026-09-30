SELECT last_restock, store_id, DAY(last_restock) AS last_restock_day
FROM inventory
WHERE store_id = 3 OR quantity > 69
ORDER BY last_restock_day, store_id DESC;
