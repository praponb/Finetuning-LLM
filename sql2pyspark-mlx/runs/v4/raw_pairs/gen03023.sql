SELECT id, region, opened_date, sqft, ROUND(AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) sqft_moving_avg_3 FROM stores;
