SELECT
    id,
    country,
    rating,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_pct_rank
FROM suppliers
ORDER BY 3 DESC;
