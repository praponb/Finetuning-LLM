SELECT id, store_id, sale_date, units, SUM(units) OVER (PARTITION BY store_id ORDER BY sale_date, id) running_units FROM store_sales WHERE product_id = 3 ORDER BY id;
