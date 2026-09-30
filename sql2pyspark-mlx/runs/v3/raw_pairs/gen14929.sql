SELECT
    id,
    region,
    opened_date,
    sqft,
    SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS running_sqft
FROM stores;
