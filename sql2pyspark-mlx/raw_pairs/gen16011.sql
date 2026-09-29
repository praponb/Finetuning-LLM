SELECT *, CAST(quantity AS DOUBLE) AS quantity_num FROM inventory WHERE store_id IN (2, 3, 4, 6) OR last_restock > '2023-12-11' ORDER BY 1 DESC, quantity;
