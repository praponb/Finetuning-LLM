SELECT id, sqft, city, opened_date, DENSE_RANK() OVER (ORDER BY sqft DESC) dense_rnk, PERCENT_RANK() OVER (PARTITION BY city ORDER BY opened_date) opened_date_pct_rank FROM stores;
