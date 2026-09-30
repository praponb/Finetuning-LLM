SELECT id, opened_date, sqft, ROUND(sqft / SUM(sqft) OVER (PARTITION BY DATE_TRUNC('month', opened_date)), 3) AS sqft_share FROM stores WHERE city IN ('Chiang Mai', 'Berlin', 'London')
