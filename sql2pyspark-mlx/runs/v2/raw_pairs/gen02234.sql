SELECT *, ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
FROM inventory
WHERE product_id IN (1, 4, 8, 10) AND product_id IN (3, 4, 5, 9);
