SELECT id, store_id, DATE_TRUNC('WEEK', sale_date) AS sale_date_period FROM store_sales WHERE store_id IN (1, 2, 5) AND (store_id IN (3, 4, 5) OR store_id IN (2, 3, 4, 5))
