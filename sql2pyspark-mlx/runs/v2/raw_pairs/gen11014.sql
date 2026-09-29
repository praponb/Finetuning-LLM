SELECT id, sale_date, PERCENT_RANK() OVER (ORDER BY sale_date) sale_date_pct_rank FROM store_sales WHERE units < 18;
