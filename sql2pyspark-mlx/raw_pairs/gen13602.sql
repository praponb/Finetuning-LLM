WITH ranked AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating
    FROM suppliers
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY rating DESC) = 1
),
kept AS (
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND SIZE(ch.tags) > 2
        )
),
enriched AS (
    SELECT
        *,
        MAX(rating) OVER w_part AS running_max_rating,
        SUM(rating) OVER w_part AS running_sum_rating,
        MIN(rating) OVER w_part AS running_min_rating
    FROM kept
    WINDOW w_part AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        MAX(rating) OVER (PARTITION BY city) AS city_max_rating,
        AVG(running_min_rating) OVER (PARTITION BY country ORDER BY running_min_rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_min_rating_avg_6_prior
    FROM enriched
    WHERE running_min_rating BETWEEN 2.6 AND 2.9
),
kept_suppliers AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price >= 690
        )
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            city,
            country,
            city_max_rating
        FROM kept_suppliers
    ) src
    PIVOT (
        SUM(city_max_rating) AS amount
        FOR country IN ('SG', 'DE')
    )
)
SELECT
    city,
    SG,
    DE
FROM by_type
ORDER BY city DESC NULLS LAST;
