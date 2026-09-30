SELECT
    id,
    opened_date,
    city,
    sqft,
    region,
    RANK() OVER (PARTITION BY YEAR(opened_date), city ORDER BY sqft) AS rnk,
    LAG(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft
FROM stores;
