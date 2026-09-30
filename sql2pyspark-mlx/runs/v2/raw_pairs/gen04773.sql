SELECT store_id, YEAR(last_restock) last_restock_year, MIN(quantity) min_quantity FROM inventory GROUP BY store_id, last_restock_year
