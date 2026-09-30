SELECT id, quantity, store_id, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE quantity BETWEEN 74 AND 125
