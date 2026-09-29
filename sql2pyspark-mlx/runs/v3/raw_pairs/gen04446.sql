SELECT id, city, sqft, PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS sqft_pct_rank FROM stores ORDER BY sqft DESC, id;
