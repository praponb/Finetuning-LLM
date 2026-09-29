SELECT
    id,
    region,
    city,
    opened_date,
    sqft,
    SUM(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS running_sqft
FROM stores;
