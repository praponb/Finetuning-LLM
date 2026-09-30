WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at,
        INITCAP(TRIM(status)) AS status_clean,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE status <> 'OPEN'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(claimed_amount) AS max_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_claimed_amount
    FROM base s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        s.status_clean,
        s.approved_amount_per_claimed_amount,
        s.max_claim_claimed_amount,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium
    FROM with_stats s
    JOIN policies p ON p.id = s.policy_id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND TO_DATE(c.reported_at) < DATE '2025-03-02'
        )
      AND s.adjuster_id IS NOT NULL
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_status,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN policy_status IS NOT NULL THEN policy_premium ELSE 0 END) AS policy_premium_policy_status_is_not_null,
        COUNT(DISTINCT policy_holder_id) AS distinct_policy_holder_ids
    FROM screened
    WHERE max_claim_claimed_amount < 40020.0
    GROUP BY policy_status
)
SELECT
    policy_status,
    distinct_ids,
    policy_premium_policy_status_is_not_null,
    distinct_policy_holder_ids
FROM rollup_base
WHERE policy_status IN ('LAPSED', 'ACTIVE', 'CANCELLED')
