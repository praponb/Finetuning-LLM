SELECT id, last_restock, LAST_DAY(last_restock) AS last_restock_month_end, DATE_TRUNC('MONTH', last_restock) AS last_restock_period FROM inventory WHERE store_id IN (3, 4) ORDER BY id DESC;
