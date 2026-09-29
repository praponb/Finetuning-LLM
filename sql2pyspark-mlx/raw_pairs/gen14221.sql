WITH filtered AS (
    SELECT *, CONCAT(country, ', ', city) AS country_city
    FROM suppliers
    WHERE country IN ('DE', 'TH', 'JP')
),
with_windows AS (
    SELECT
        id,
        city,
        rating,
        country_city,
        MIN(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_rating,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM filtered
    WHERE supplier_name IS NOT NULL
),
with_windows_suppliers AS (
    SELECT
        id,
        last4_sum_rating,
        last4_avg_rating,
        NTILE(10) OVER (ORDER BY last4_sum_rating, id) AS last4_sum_rating_bucket,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM with_windows
),
eligible AS (
    SELECT s.*
    FROM with_windows_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost < 380
        )
),
kept AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price IS NOT NULL
        )
),
stats AS (
    SELECT
        CASE WHEN last4_sum_rating >= 18.4 THEN 'LARGE' ELSE 'SMALL' END AS last4_sum_rating_tier,
        CASE WHEN last4_avg_rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_avg_rating_tier,
        MIN(last4_sum_rating) AS min_last4_sum_rating,
        COUNT(*) AS row_count,
        COUNT(*) AS num_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(last4_avg_rating, 0.5) AS last4_avg_rating_p50
    FROM kept
    GROUP BY GROUPING SETS ((CASE WHEN last4_sum_rating >= 18.4 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last4_avg_rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN last4_sum_rating >= 18.4 THEN 'LARGE' ELSE 'SMALL' END))
)
SELECT
    last4_sum_rating_tier,
    min_last4_sum_rating,
    row_count,
    distinct_ids,
    last4_avg_rating_p50
FROM stats;
