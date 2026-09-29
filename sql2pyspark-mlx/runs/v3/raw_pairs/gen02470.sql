SELECT product_id, quantity, ADD_MONTHS(last_restock, 12) AS last_restock_plus_months FROM inventory WHERE quantity != 173 AND store_id = 4;
