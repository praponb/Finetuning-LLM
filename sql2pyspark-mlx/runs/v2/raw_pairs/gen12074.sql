SELECT store_id, product_id, quantity, ADD_MONTHS(last_restock, 1) AS last_restock_plus_months FROM inventory WHERE product_id IN (2, 9);
