SELECT id, store_id, sale_date, units, LEAD(units, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) next_units FROM store_sales WHERE store_id IN (1, 2, 6) ORDER BY id DESC;
