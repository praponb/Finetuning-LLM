SELECT id, last_restock, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE product_id IN (4, 5) ORDER BY last_restock;
