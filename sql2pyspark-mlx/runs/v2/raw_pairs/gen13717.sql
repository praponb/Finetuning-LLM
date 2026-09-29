SELECT id, region, sqft, PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft) AS sqft_pct_rank FROM stores;
