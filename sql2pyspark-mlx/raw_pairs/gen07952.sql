SELECT
    city,
    region,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    SUM(CASE WHEN region = 'North' THEN 1 ELSE 0 END) AS north_count,
    MAX(sqft) AS max_sqft
FROM stores
GROUP BY CUBE(city, region);
