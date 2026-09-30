SELECT last_restock, DATE_TRUNC('WEEK', last_restock) AS last_restock_period, ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
FROM inventory
WHERE store_id IN (4, 5) AND YEAR(last_restock) = 2023;
