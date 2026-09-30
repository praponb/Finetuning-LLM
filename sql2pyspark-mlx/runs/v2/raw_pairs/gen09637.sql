SELECT
    id,
    city,
    opened_date,
    sqft,
    LEAD(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_sqft,
    SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id) AS running_sqft
FROM stores;
