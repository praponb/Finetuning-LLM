SELECT store_id, DATE_TRUNC('YEAR', last_restock) AS last_restock_period FROM inventory
