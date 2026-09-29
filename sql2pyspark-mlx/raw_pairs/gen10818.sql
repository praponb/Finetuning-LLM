WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        rating,
        LENGTH(country) AS country_length,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM suppliers
    WHERE city = 'London'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name IS NOT NULL
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(country_length, 1) OVER (PARTITION BY country ORDER BY country_length, id) AS prev_country_length,
        FIRST_VALUE(rating, TRUE) OVER (PARTITION BY country ORDER BY country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        CUME_DIST() OVER (PARTITION BY country ORDER BY country_length NULLS FIRST) AS country_length_cume_dist,
        LAST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY country ORDER BY country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        NTILE(3) OVER (PARTITION BY country ORDER BY rating, id) AS rating_bucket
    FROM not_flagged
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(rating, 1) OVER (PARTITION BY country ORDER BY last_known_rating, id) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY last_known_rating, id) AS row_seq,
        FIRST_VALUE(first_known_rating) IGNORE NULLS OVER (PARTITION BY country ORDER BY country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_rating
    FROM metrics
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    country,
    metric_name,
    value
FROM (
    SELECT
        id,
        country,
        prev_rating,
        first_known_rating,
        rating
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (prev_rating AS PREV_RATING_VAL, first_known_rating AS FIRST_KNOWN_RATING_VAL, rating AS RATING)
)
