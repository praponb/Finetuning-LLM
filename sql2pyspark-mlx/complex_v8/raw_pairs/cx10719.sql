WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        ABS(premium - 3532.7) AS premium_distance
    FROM policies
    WHERE premium > 1042.8
),
enriched AS (
    -- Add calculated columns
    SELECT *, ABS(premium_distance - 3500.0) AS premium_distance_distance
    FROM base_policys
    WHERE end_date < DATE '2026-12-05'
),
grouped AS (
    -- Summarise per group
    SELECT
        holder_id,
        ROUND(SUM(premium_distance), 2) AS total_premium_distance,
        ROUND(AVG(premium_distance), 2) AS avg_premium_distance,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        ROUND(100 * COUNT(CASE WHEN product <> 'HEALTH' THEN 1 END) / COUNT(*), 1) AS pct_product_health,
        MIN(premium_distance_distance) AS min_premium_distance_distance
    FROM enriched
    GROUP BY 1
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM grouped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'TRAVEL'
              AND p.end_date IS NOT NULL
        )
),
detail_rows AS (
    -- One row per policy
    SELECT
        s.holder_id,
        s.total_premium_distance,
        s.avg_premium_distance,
        s.pct_product_health,
        s.min_premium_distance_distance,
        p.id AS policy_id,
        p.end_date AS policy_end_date,
        p.status AS policy_status
    FROM with_activity s
    LEFT JOIN policies p ON p.holder_id = s.holder_id
)
-- Final projection
SELECT
    holder_id,
    total_premium_distance,
    avg_premium_distance,
    pct_product_health,
    min_premium_distance_distance,
    policy_end_date,
    policy_status,
    CASE WHEN pct_product_health >= 85.0 THEN 'large' WHEN pct_product_health >= 30.0 THEN 'medium' ELSE 'small' END AS pct_product_health_tier,
    ROUND(total_premium_distance * 0.25, 2) AS total_premium_distance_adj
FROM detail_rows
WHERE policy_status <> 'CANCELLED'
ORDER BY policy_status, total_premium_distance DESC NULLS FIRST
