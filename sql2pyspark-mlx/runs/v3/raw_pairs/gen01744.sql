SELECT id, store_id, units, AVG(units) OVER (PARTITION BY store_id) store_id_avg_units FROM store_sales ORDER BY store_id_avg_units
