WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id) AS prev_sqft,
        LEAD(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft, id) AS seq_no,
        SUM(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM stores
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    prev_sqft,
    next_sqft
FROM metrics
ORDER BY city NULLS LAST;
