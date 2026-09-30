SELECT
    QUARTER(opened_date) AS opened_quarter,
    city,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    SUM(CASE WHEN region = 'West' THEN sqft ELSE 0 END) AS west_sqft
FROM stores
WHERE sqft BETWEEN 6323 AND 11232
GROUP BY QUARTER(opened_date), city;
