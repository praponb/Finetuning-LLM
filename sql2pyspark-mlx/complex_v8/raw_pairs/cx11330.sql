WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
),
unique_rows AS (
    -- Keep the earliest row per adjuster_id, claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, claim_type ORDER BY claimed_amount, id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
policy_id_stats AS (
    -- Median approved_amount per policy_id
    SELECT policy_id, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM unique_rows
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM unique_rows s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount >= g.median_approved_amount
),
qualified AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM vs_group s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.premium BETWEEN 1051.9 AND 4417.0
        )
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(status, 'ALL') AS status,
    policy_id,
    GROUPING_ID() AS gid,
    MAX(claimed_amount) AS max_claimed_amount,
    MIN(reported_at) AS first_reported_at
FROM qualified
GROUP BY ROLLUP(status, policy_id)
ORDER BY status NULLS LAST, policy_id NULLS LAST
