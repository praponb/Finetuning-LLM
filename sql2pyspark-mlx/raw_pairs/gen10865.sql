SELECT id, opened_date, city, sqft, ROUND(sqft / SUM(sqft) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date), city), 3) AS sqft_share FROM stores ORDER BY id DESC;
