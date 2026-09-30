SELECT store_id, DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, MAX(quantity) AS max_quantity FROM inventory GROUP BY store_id, last_restock_month;
