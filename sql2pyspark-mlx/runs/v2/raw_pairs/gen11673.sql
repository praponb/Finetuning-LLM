SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE store_id IN (3, 4) OR MONTH(last_restock) IN (4, 7, 9)
