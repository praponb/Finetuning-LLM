WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        CASE WHEN status IN ('LAPSED', 'ACTIVE') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group,
        ABS(premium - 2319.8) AS premium_distance,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM policies
    WHERE holder_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        premium - LAG(premium) OVER (PARTITION BY product ORDER BY premium ASC, id) AS premium_change,
        ROUND(CUME_DIST() OVER (PARTITION BY status_group ORDER BY premium_distance DESC, id), 3) AS premium_distance_cume_dist,
        LAG(premium) OVER (PARTITION BY status ORDER BY premium_distance DESC, id) AS prev_premium,
        premium - LAG(premium) OVER (PARTITION BY status ORDER BY premium_distance DESC, id) AS premium_change_2,
        ROUND(AVG(premium) OVER (PARTITION BY product ORDER BY premium ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2
    FROM scoped_policys
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(premium_avg_2 / 1000) * 1000 AS premium_avg_2_bucket,
        CASE WHEN premium >= 3544.1 THEN 'A' WHEN premium >= 2289.0 THEN 'B' ELSE 'C' END AS premium_tier,
        CONCAT(status_group, '#', CAST(holder_id AS STRING)) AS status_group_ref
    FROM sequenced
    WHERE premium_change <= 500.0
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.status_group,
        s.status_prefix,
        s.premium_change,
        s.premium_distance_cume_dist,
        s.prev_premium,
        s.premium_change_2,
        s.premium_avg_2,
        s.premium_avg_2_bucket,
        s.status_group_ref,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment,
        p.holder_name
    FROM labeled s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_details s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
)
SELECT
    COALESCE(status, 'ALL') AS status,
    COALESCE(status_group, 'ALL') AS status_group,
    MIN(prev_premium) AS min_prev_premium
FROM with_activity
GROUP BY ROLLUP(status, status_group)
ORDER BY status NULLS LAST, status_group NULLS LAST
