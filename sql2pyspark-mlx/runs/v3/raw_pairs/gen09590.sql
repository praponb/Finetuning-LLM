SELECT *, ADD_MONTHS(last_restock, 3) AS last_restock_plus_months, FLOOR(quantity) AS quantity_whole
FROM inventory;
