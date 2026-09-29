SELECT id, category, brand, launch_date, PERCENT_RANK() OVER (PARTITION BY category, brand ORDER BY launch_date) AS launch_date_pct_rank FROM products
