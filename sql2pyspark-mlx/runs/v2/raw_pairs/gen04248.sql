SELECT store_id, QUARTER(last_restock) AS last_restock_quarter, COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
WHERE quantity < 62
GROUP BY store_id, last_restock_quarter
