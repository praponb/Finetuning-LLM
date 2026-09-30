SELECT id, city, sqft, RANK() OVER (PARTITION BY city ORDER BY sqft) AS sqft_rank FROM stores ORDER BY sqft_rank
