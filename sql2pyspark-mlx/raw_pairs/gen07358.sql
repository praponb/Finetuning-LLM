SELECT last_restock, quantity, ROUND(quantity, 2) AS quantity_rounded, ADD_MONTHS(last_restock, 12) AS last_restock_plus_months FROM inventory WHERE quantity BETWEEN 71 AND 193;
