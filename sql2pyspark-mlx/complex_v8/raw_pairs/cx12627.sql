WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        CASE WHEN premium >= 3532.7 THEN 'HIGH' WHEN premium >= 2013.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE status IS NOT NULL
      AND start_date >= DATE '2024-06-19'
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date BETWEEN DATE '2024-06-26' AND DATE '2025-04-20'
              AND c.approved_amount <= 32510.7
        )
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.premium_filled,
        s.premium_tier,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM without_matches s
    JOIN policyholders p ON p.id = s.holder_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        premium_tier,
        ARRAY_SORT(COLLECT_SET(holder_name)) AS holder_name_list,
        COUNT(*) AS n_rows
    FROM with_details
    GROUP BY premium_tier
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY n_rows DESC, premium_tier) AS rn,
        COUNT(*) OVER w_premium_tier AS rows_in_premium_tier,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY premium_tier), 0), 4) AS n_rows_share_of_premium_tier,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows ORDER BY n_rows), 3) AS n_rows_cume_dist
    FROM rollup_base
    WINDOW w_premium_tier AS (PARTITION BY premium_tier)
)
SELECT
    premium_tier,
    holder_name_list,
    n_rows,
    rn,
    n_rows_share_of_premium_tier,
    LENGTH(TRIM(premium_tier)) AS premium_tier_len
FROM windowed
ORDER BY rn DESC NULLS FIRST, n_rows_share_of_premium_tier DESC
