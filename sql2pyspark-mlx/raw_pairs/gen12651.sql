WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5
    FROM suppliers
)
SELECT
    id,
    running_rating,
    rating_avg_5
FROM scored
ORDER BY running_rating NULLS LAST, id DESC;
