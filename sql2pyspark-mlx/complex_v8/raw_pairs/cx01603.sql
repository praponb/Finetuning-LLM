WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type = 'flood'
              AND c.approved_amount >= 5336.7
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_policy_id_by_reported_at AS reported_at_seq,
        RANK() OVER (PARTITION BY policy_id ORDER BY reported_at ASC NULLS LAST, id) AS reported_at_rank,
        ROUND(CUME_DIST() OVER w_policy_id_by_reported_at, 3) AS reported_at_cume_dist,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_claimed_amount
    FROM screened
    WINDOW w_policy_id_by_reported_at AS (PARTITION BY policy_id ORDER BY reported_at ASC NULLS LAST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        reported_at_rank,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM metrics
    WHERE claimed_amount >= 17585.2
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        days_since_reported_at
    FROM enriched
    WHERE reported_at_rank <= 2
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    reported_at,
    days_since_reported_at,
    FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
    DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label
FROM top_rows
ORDER BY adjuster_id, id
LIMIT 20
