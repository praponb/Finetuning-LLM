WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CASE WHEN status IN ('APPROVED', 'PAID') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
        MONTH(reported_at) AS reported_at_month
    FROM claims
    WHERE status = 'APPROVED'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(reported_at_month, 2) OVER (ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_month,
        COUNT(*) OVER (PARTITION BY reported_at_month) AS rows_in_reported_at_month
    FROM base_claims
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount < 20880.4
        )
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.status_group,
        s.reported_at_month,
        s.second_reported_at_month,
        s.rows_in_reported_at_month,
        p.start_date AS policy_start_date
    FROM screened s
    JOIN policies p ON p.id = s.policy_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.reported_at < TIMESTAMP '2025-02-24 12:00:00'
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        COUNT(*) AS claim_count_2,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2,
        c.first_claim_date
    FROM filtered s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    approved_amount,
    reported_at,
    second_reported_at_month,
    rows_in_reported_at_month,
    policy_start_date,
    claim_count_2,
    first_claim_date,
    DATEDIFF(first_claim_date, policy_start_date) AS days_policy_start_date_to_first_claim_date
FROM with_counts
ORDER BY claimed_amount DESC NULLS FIRST
