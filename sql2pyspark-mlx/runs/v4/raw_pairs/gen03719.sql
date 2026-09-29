SELECT id, product_id, DAY(last_restock) AS last_restock_day, DATE_TRUNC('WEEK', last_restock) AS last_restock_period FROM inventory WHERE quantity <= 176
