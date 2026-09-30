WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        LAST_VALUE(city) IGNORE NULLS OVER w_hist AS last_known_city
    FROM suppliers
    WINDOW w_hist AS (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    city,
    running_rating,
    last_known_city
FROM with_windows
ORDER BY city DESC
