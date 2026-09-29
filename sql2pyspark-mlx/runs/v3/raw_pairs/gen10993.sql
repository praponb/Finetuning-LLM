SELECT revenue, product_id, DATE_TRUNC('MONTH', sale_date) sale_date_period, DAY(sale_date) sale_date_day FROM store_sales WHERE store_id IN (1, 2, 6) AND product_id IN (1, 7);
