SELECT
    MONTH(opened_date) AS opened_month,
    region,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SUM(CASE WHEN region = 'West' THEN sqft ELSE 0 END) AS west_sqft,
    MAX(opened_date) AS latest_opened_date
FROM stores
GROUP BY opened_month, region
ORDER BY opened_month, 4 DESC, region
LIMIT 10;
