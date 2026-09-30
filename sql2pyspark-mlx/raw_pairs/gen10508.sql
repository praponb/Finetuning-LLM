WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_2
    FROM suppliers
)
SELECT
    id,
    country,
    city,
    rating
FROM scored
ORDER BY rating DESC NULLS LAST;
