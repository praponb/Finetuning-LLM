SELECT id, city, opened_date, sqft, ROUND(AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sqft_moving_avg_3 FROM stores
