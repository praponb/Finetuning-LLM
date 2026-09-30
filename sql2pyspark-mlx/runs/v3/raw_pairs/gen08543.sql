SELECT id, last_restock, quantity, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE store_id = 6 AND last_restock < '2025-03-01'
