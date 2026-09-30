SELECT id, store_id, sale_date, PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) sale_date_pct_rank FROM store_sales ORDER BY sale_date;
