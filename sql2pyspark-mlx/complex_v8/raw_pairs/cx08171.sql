WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d,
        ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m,
        ABS(claimed_amount - 20505.7) AS claimed_amount_distance
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at_plus_1d,
        claim_date_plus_6m,
        claimed_amount_distance,
        LEAD(claim_date_plus_6m) OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount_distance DESC, id) AS next_claim_date_plus_6m,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        ROUND(CUME_DIST() OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount_distance DESC), 3) AS claimed_amount_distance_cume_dist
    FROM base
)
-- Subtotals with GROUPING SETS
SELECT
    status,
    policy_id,
    adjuster_id,
    COUNT(*) AS row_count
FROM metrics
GROUP BY GROUPING SETS ((status, policy_id, adjuster_id), (status))
ORDER BY status NULLS LAST, policy_id NULLS LAST, adjuster_id NULLS LAST
