SELECT id, store_id, quantity, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity) AS dense_rnk FROM inventory WHERE YEAR(last_restock) = 2025 ORDER BY store_id
