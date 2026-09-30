SELECT region, DATE_TRUNC('MONTH', opened_date) AS opened_month, city, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY region, DATE_TRUNC('MONTH', opened_date), city;
