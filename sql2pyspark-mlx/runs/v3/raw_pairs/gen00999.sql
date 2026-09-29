SELECT id, city, opened_date, sqft, NTILE(10) OVER (PARTITION BY city ORDER BY opened_date, id) AS quartile, FIRST_VALUE(sqft) OVER (ORDER BY opened_date, id) AS first_sqft FROM stores ORDER BY sqft;
