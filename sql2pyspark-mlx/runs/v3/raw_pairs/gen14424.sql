SELECT *, ROUND(quantity * 1.1, 2) AS quantity_adjusted FROM inventory WHERE store_id IN (4, 6) AND last_restock > '2023-10-01' AND product_id IN (2, 5, 8);
