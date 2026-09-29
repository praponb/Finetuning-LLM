SELECT
    region,
    MONTH(opened_date) AS opened_month,
    SUM(CASE WHEN region = 'North' THEN 1 ELSE 0 END) AS north_count,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE region IN ('North', 'South', 'East')
GROUP BY 1, 2
HAVING ROUND(STDDEV(sqft), 2) >= 66;
