SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE (product_id = 1 AND store_id = 2) OR store_id IN (2, 5);
