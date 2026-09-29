WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        rating,
        LOWER(country) AS country_lower
    FROM suppliers
    WHERE rating BETWEEN 1.8 AND 2.7
)
-- Ranking and gap features
SELECT
    id,
    supplier_name,
    country_lower,
    STDDEV_SAMP(rating) OVER w_ordered AS country_stddev_samp_rating,
    SUM(rating) OVER w_hist AS running_rating,
    MAX(rating) OVER w_ordered AS country_max_rating,
    LEAD(rating, 2) OVER w_running AS next_rating
FROM filtered
WINDOW w_ordered AS (PARTITION BY country), w_hist AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY rating, id);
