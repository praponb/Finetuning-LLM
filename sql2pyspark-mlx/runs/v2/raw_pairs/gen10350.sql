SELECT
    id,
    city,
    country,
    rating,
    PERCENT_RANK() OVER (PARTITION BY city, country ORDER BY rating DESC) AS rating_pct_rank
FROM suppliers
WHERE city = 'Bangkok'
