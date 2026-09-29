SELECT
    id,
    opened_date,
    region,
    sqft,
    RANK() OVER (ORDER BY opened_date DESC) AS rnk,
    MIN(sqft) OVER (PARTITION BY region) AS region_min_sqft
FROM stores
ORDER BY region;
