SELECT
    id,
    region,
    opened_date,
    sqft,
    SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS running_sqft,
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY opened_date) AS opened_date_pct_rank
FROM stores
WHERE sqft BETWEEN 10398 AND 12608
