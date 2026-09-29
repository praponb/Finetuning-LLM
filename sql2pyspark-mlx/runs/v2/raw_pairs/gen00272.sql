SELECT *, ADD_MONTHS(last_restock, 3) last_restock_plus_months, LAST_DAY(last_restock) last_restock_month_end FROM inventory WHERE store_id IN (2, 5, 6) AND store_id IN (3, 5, 6);
