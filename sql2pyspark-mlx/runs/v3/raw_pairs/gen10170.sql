SELECT
    id,
    city,
    opened_date,
    sqft,
    region,
    LEAD(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) next_sqft,
    LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) next_sqft_2
FROM stores;
