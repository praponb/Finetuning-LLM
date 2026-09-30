SELECT id, units, store_id, COALESCE(units, 0) AS units_filled, DATE_TRUNC('YEAR', sale_date) AS sale_date_period FROM store_sales WHERE store_id = 6 AND store_id IN (2, 3, 4, 6);
