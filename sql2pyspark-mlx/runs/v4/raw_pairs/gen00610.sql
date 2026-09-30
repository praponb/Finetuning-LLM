SELECT id, city, sqft, ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 3) AS sqft_share FROM stores WHERE opened_date <= '2023-12-01';
