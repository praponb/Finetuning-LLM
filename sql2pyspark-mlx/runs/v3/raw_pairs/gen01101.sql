SELECT *, CAST(quantity AS DOUBLE) AS quantity_num, DATE_TRUNC('YEAR', last_restock) AS last_restock_period
FROM inventory
ORDER BY quantity, id;
