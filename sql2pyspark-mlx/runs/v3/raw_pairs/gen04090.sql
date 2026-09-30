SELECT store_id, YEAR(last_restock) AS last_restock_year, MAX(quantity) AS max_quantity FROM inventory GROUP BY store_id, last_restock_year ORDER BY store_id DESC
