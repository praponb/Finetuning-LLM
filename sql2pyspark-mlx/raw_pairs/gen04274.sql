SELECT quantity, last_restock, store_id, DAY(last_restock) AS last_restock_day, ADD_MONTHS(last_restock, 6) AS last_restock_plus_months FROM inventory WHERE store_id IN (4, 5) ORDER BY store_id DESC;
