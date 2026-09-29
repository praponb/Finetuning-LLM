SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, store_id, COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
GROUP BY 1, 2
HAVING COUNT(DISTINCT quantity) >= 4
