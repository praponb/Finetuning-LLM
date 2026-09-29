SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, ROUND(STDDEV(sqft), 2) AS stddev_sqft, MIN(sqft) AS min_sqft
FROM stores
WHERE store_name IS NOT NULL
GROUP BY 1;
