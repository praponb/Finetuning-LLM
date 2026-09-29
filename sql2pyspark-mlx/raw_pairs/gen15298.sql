SELECT QUARTER(opened_date) AS opened_quarter, CASE WHEN sqft >= 13517 THEN 'high' WHEN sqft >= 7544 THEN 'mid' ELSE 'low' END AS sqft_bucket, MIN(sqft) AS min_sqft
FROM stores
WHERE region IS NULL AND sqft = 9149
GROUP BY CUBE(QUARTER(opened_date), CASE WHEN sqft >= 13517 THEN 'high' WHEN sqft >= 7544 THEN 'mid' ELSE 'low' END)
ORDER BY min_sqft DESC;
