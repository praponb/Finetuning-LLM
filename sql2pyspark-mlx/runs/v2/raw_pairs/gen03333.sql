SELECT
    region,
    QUARTER(opened_date) AS opened_quarter,
    AVG(sqft) AS avg_sqft,
    MIN(sqft) AS min_sqft
FROM stores
WHERE store_name IS NOT NULL
GROUP BY region, QUARTER(opened_date);
