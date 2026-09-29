SELECT
    region,
    AVG(sqft) AS avg_sqft,
    MAX(opened_date) AS latest_opened_date,
    MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY region
