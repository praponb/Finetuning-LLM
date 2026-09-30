WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        city,
        rating,
        INITCAP(TRIM(supplier_name)) AS supplier_name_title,
        COALESCE(city, 'NONE') AS resolved_city
    FROM suppliers
    WHERE country = 'SG'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        rating,
        supplier_name_title,
        resolved_city,
        dedup_rank,
        LAG(rating) OVER w_grp AS prev_rating,
        LEAD(rating) OVER w_grp AS next_rating,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM latest
    WHERE rating >= 2.3
    WINDOW w_grp AS (PARTITION BY city ORDER BY dedup_rank, id)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name IS NOT NULL
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END AS next_rating_tier,
    PERCENTILE_APPROX(next_rating, ARRAY(0.25, 0.5, 0.75)) AS next_rating_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(prev_rating) AS max_prev_rating,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3,
    AVG(next_rating) AS avg_next_rating
FROM eligible
GROUP BY CASE WHEN next_rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids_3 NULLS FIRST
