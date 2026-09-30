SELECT id, product_id, ROUND(quantity, 2) AS quantity_rounded FROM inventory WHERE product_id IN (2, 3, 5, 8) ORDER BY 1;
