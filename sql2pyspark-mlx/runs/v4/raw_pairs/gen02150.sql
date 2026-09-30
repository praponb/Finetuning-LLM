WITH base AS (
    SELECT
        id,
        city,
        rating,
        UPPER(city) AS city_upper,
        CONCAT_WS('/', city, country) AS city_country_key,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE LOWER(TRIM(supplier_name)) = 'lion city imports'
),
base_suppliers AS (
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM base
    WHERE city IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM base_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.category = 'Sports'
        )
),
scored AS (
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        LAG(rating, 1) OVER (ORDER BY rating, id) AS prev_rating,
        COUNT(rating) OVER (PARTITION BY city) AS city_count_rating
    FROM not_flagged
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(rating_share) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating_share,
        ROUND(city_count_rating / SUM(city_count_rating) OVER (PARTITION BY city), 4) AS city_count_rating_share,
        AVG(rating) OVER (ORDER BY rating_share, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5
    FROM scored
),
with_last AS (
    SELECT s.*, top1.launch_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(rating, TRUE) OVER (PARTITION BY city ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        LAG(prev_rating, 1) OVER (PARTITION BY city ORDER BY launch_date, id) AS prev_prev_rating,
        RANK() OVER (PARTITION BY city ORDER BY rating_share DESC) AS rating_share_rank,
        ROUND(rating_avg_5 / SUM(rating_avg_5) OVER (PARTITION BY city), 4) AS rating_avg_5_share,
        NTILE(3) OVER (PARTITION BY city ORDER BY city_count_rating_share DESC, id) AS city_count_rating_share_bucket
    FROM with_last
)
SELECT
    *,
    AVG(rating_avg_5_share) OVER (PARTITION BY city ORDER BY rating_share, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_5_share_avg_2_prior,
    LAST_VALUE(prev_rating, TRUE) OVER (ORDER BY first_known_rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_rating,
    FIRST_VALUE(city_count_rating_share, TRUE) OVER (PARTITION BY city ORDER BY rating_share_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city_count_rating_share,
    SUM(rating_avg_5_share) OVER (PARTITION BY city ORDER BY rating_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_avg_5_share,
    LEAD(prev_prev_rating, 1) OVER (PARTITION BY city ORDER BY city_count_rating_share, id) AS next_prev_prev_rating
FROM with_windows
