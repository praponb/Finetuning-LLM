SELECT
    region,
    COUNT(DISTINCT sqft) AS unique_regions,
    AVG(sqft) AS avg_sqft,
    MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY region
