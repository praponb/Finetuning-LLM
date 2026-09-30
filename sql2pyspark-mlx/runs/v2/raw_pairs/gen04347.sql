SELECT quantity, DATE_TRUNC('year', last_restock) AS last_restock_period FROM inventory;
