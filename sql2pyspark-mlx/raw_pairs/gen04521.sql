WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating,
        STDDEV_SAMP(rating) OVER (PARTITION BY country) AS country_stddev_samp_rating
    FROM suppliers
)
SELECT
    id,
    city,
    next_rating
FROM metrics
ORDER BY city
