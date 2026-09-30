SELECT store_id, YEAR(last_restock) last_restock_year, MAX(last_restock) last_last_restock
FROM inventory
WHERE quantity <> 81
GROUP BY store_id, YEAR(last_restock)
ORDER BY last_restock_year ASC, store_id ASC
LIMIT 1
