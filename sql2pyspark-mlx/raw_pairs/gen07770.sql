WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM suppliers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_grp AS cumulative_rating_cume_dist,
        NTILE(3) OVER (PARTITION BY country, city ORDER BY rating_share DESC, id) AS rating_share_bucket,
        SUM(cumulative_rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_rating,
        LAG(rating, 2) OVER w_running AS prev_rating
    FROM metrics
    WINDOW w_grp AS (PARTITION BY city ORDER BY cumulative_rating NULLS LAST), w_running AS (PARTITION BY city ORDER BY rating, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY cumulative_rating_cume_dist, id) AS rn
    FROM scored
    QUALIFY rn = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category != 'Grocery'
        )
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category != 'Grocery'
        )
)
-- Grouped metrics
SELECT
    CASE WHEN rating_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS rating_share_tier,
    city,
    ROUND(SUM(cumulative_cumulative_rating) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS cumulative_cumulative_rating_per_berlin,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    SIZE(COLLECT_SET(country)) AS num_countries,
    GROUPING_ID() AS grouping_id
FROM kept_suppliers
GROUP BY ROLLUP (CASE WHEN rating_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END, city)
ORDER BY city DESC NULLS FIRST
