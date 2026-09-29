WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE status <> 'IN_REVIEW'
      AND claim_type IN ('medical', 'collision')
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.holder_id AS policy_holder_id
    FROM recent_claims s
    JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount DESC, id), 3) AS approved_amount_percent_rank,
        NTILE(10) OVER w_all_by_approved_amount AS approved_amount_decile,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type) AS distinct_policy_id_in_claim_type,
        LAG(reported_at) OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id) AS prev_reported_at,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_status, 0), 4) AS approved_amount_share_of_status,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_status, 0), 4) AS claimed_amount_share_of_status,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS rows_in_claim_date_month
    FROM with_parent_policy
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_claim_type AS (PARTITION BY claim_type), w_status AS (PARTITION BY status)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        approved_amount_decile,
        adjuster_id,
        ROUND(SUM(approved_amount) / NULLIF(SUM(approved_amount_decile), 0), 3) AS approved_amount_to_approved_amount_decile_ratio,
        MIN(claim_date) AS first_claim_date
    FROM windowed
    GROUP BY policy_id, approved_amount_decile, adjuster_id
    HAVING AVG(claimed_amount_share_of_status) > 0.3
)
-- Final projection
SELECT
    policy_id,
    approved_amount_decile,
    adjuster_id,
    approved_amount_to_approved_amount_decile_ratio,
    first_claim_date,
    MONTH(first_claim_date) AS first_claim_date_month,
    DATE_TRUNC('WEEK', first_claim_date) AS first_claim_date_week
FROM stats
WHERE approved_amount_decile < 7
ORDER BY first_claim_date_month, approved_amount_decile DESC
