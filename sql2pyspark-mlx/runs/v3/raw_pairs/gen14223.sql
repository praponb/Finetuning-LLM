SELECT
    id,
    city,
    rating,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating) AS rating_pct_rank
FROM suppliers
