SELECT region, city, YEAR(opened_date) AS opened_year, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY region, city, opened_year;
