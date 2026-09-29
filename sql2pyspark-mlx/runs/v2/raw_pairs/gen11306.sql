SELECT MONTH(opened_date) AS opened_month, ROUND(STDDEV(sqft), 2) AS stddev_sqft, MIN(sqft) AS min_sqft FROM stores WHERE sqft <> 12201 AND region IS NOT NULL GROUP BY MONTH(opened_date);
