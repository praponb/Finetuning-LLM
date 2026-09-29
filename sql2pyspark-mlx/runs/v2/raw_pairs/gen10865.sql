SELECT
    id,
    city,
    opened_date,
    sqft,
    region,
    LEAD(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_sqft,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS row_num
FROM stores
ORDER BY sqft DESC, opened_date;
