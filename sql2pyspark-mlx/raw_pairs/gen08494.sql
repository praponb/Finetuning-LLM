SELECT id, sale_date, store_id, revenue, MAX(revenue) OVER (PARTITION BY DATE_TRUNC('week', sale_date), store_id) AS sale_date_period_max_revenue FROM store_sales;
