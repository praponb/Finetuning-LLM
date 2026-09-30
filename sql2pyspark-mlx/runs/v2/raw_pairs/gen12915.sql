SELECT store_id, MONTH(last_restock) AS last_restock_month, MAX(quantity) AS max_quantity FROM inventory WHERE store_id IN (4, 5) GROUP BY store_id, last_restock_month;
