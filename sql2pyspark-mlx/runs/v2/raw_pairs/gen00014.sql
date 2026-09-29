SELECT
    id,
    region,
    city,
    opened_date,
    sqft,
    PERCENT_RANK() OVER (PARTITION BY region, city ORDER BY opened_date) opened_date_pct_rank,
    RANK() OVER (PARTITION BY region ORDER BY sqft DESC) sqft_rank
FROM stores
WHERE region IS NULL
ORDER BY opened_date_pct_rank
