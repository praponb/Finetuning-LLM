SELECT id, product_id, store_id, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE product_id IN (1, 3, 5, 6) AND product_id IN (8, 9);
