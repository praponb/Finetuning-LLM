SELECT last_restock, quantity, store_id, LAST_DAY(last_restock) AS last_restock_month_end, ADD_MONTHS(last_restock, 1) AS last_restock_plus_months FROM inventory ORDER BY quantity DESC;
