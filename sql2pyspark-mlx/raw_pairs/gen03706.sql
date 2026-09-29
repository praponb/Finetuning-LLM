WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        city,
        rating,
        CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai
    FROM suppliers
    WHERE city IS NOT NULL
      AND LENGTH(supplier_name) >= 8
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price < 470
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, rating
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(rating, 2) AS rating_rounded
    FROM ranked
    WHERE rating BETWEEN 2.3 AND 3.7
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        rating,
        LAG(rating, 1, 0) OVER (ORDER BY rating, id) AS prev_rating,
        SUM(rating) OVER w_running AS cumulative_rating,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior,
        SUM(rating_rounded) OVER w_running AS running_rating_rounded
    FROM valid
    WINDOW w_running AS (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_rating_rounded >= 404 THEN 'HIGH' ELSE 'LOW' END AS running_rating_rounded_tier,
    APPROX_PERCENTILE(cumulative_rating, 0.95) AS cumulative_rating_p95,
    PERCENTILE_APPROX(cumulative_rating, 0.9) AS cumulative_rating_p90,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(rating) AS rating_stddev,
    APPROX_PERCENTILE(prev_rating, 0.9) AS prev_rating_p90,
    COUNT(*) AS cnt
FROM enriched
GROUP BY running_rating_rounded_tier
HAVING distinct_ids > 1
ORDER BY cnt DESC NULLS LAST;
