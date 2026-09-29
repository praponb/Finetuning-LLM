SELECT
    city,
    MIN(opened_date) AS min_opened_date,
    SUM(sqft) AS total_sqft,
    MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY city
