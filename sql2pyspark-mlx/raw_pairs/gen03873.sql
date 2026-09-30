WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CUME_DIST() OVER (ORDER BY rating DESC NULLS LAST) AS rating_cume_dist,
        NTILE(10) OVER (ORDER BY rating, id) AS rating_bucket
    FROM suppliers
    WHERE city <> 'Tokyo'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND SIZE(ch.tags) >= 2
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price BETWEEN 950 AND 1020
        )
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.cost
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_cume_dist DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost >= 720 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    city,
    ROUND(AVG(cost), 2) AS avg_cost,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(rating) FILTER (WHERE city = 'Berlin') AS berlin_total_rating,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    ROUND(SUM(rating_cume_dist) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS rating_cume_dist_per_chiang_mai,
    GROUPING(CASE WHEN cost >= 720 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN cost >= 720 THEN 'LARGE' ELSE 'SMALL' END, city), (CASE WHEN cost >= 720 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY berlin_total_rating;
