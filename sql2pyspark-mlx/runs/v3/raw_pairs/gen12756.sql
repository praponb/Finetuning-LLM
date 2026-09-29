SELECT
    id,
    country,
    rating,
    city,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_pct_rank,
    RANK() OVER (PARTITION BY city ORDER BY rating) AS rating_rank
FROM suppliers
ORDER BY city
