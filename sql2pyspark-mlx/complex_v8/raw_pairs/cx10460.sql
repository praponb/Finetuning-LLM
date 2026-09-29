WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status
    FROM claims
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at
    FROM recent_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_claim_date) AS claim_claim_date_month,
        adjuster_id,
        MAX(claim_approved_amount) AS max_claim_approved_amount,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MAX(claimed_amount) AS max_claimed_amount,
        MIN(claim_reported_at) AS first_claim_reported_at
    FROM with_children
    WHERE claim_id <= 32
    GROUP BY DATE_TRUNC('MONTH', claim_claim_date), adjuster_id
)
-- Final projection
SELECT
    claim_claim_date_month,
    adjuster_id,
    max_claim_approved_amount,
    distinct_adjuster_ids,
    max_claimed_amount,
    first_claim_reported_at,
    FLOOR(distinct_adjuster_ids / 2) * 2 AS distinct_adjuster_ids_bucket,
    DATEDIFF(DATE '2025-12-31', claim_claim_date_month) AS days_since_claim_claim_date_month
FROM grouped
WHERE max_claim_approved_amount BETWEEN 2500.0 AND 45000.0
ORDER BY claim_claim_date_month DESC, max_claim_approved_amount NULLS LAST
