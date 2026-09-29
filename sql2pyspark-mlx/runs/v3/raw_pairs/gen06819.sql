SELECT
    region,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    MAX(opened_date) AS latest_opened_date
FROM stores
GROUP BY region
HAVING MAX(sqft) - MIN(sqft) > 73;
