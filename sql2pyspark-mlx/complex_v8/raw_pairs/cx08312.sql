WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_type = 'flood'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE status = 'APPROVED') AS claim_status_count,
        COUNT(*) FILTER (WHERE claim_type <> 'delay') AS claim_claim_type_count,
        SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        c.total_claim_claimed_amount
    FROM cleaned_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_adj,
        s.claim_status_count,
        s.total_claim_claimed_amount,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status
    FROM summarized_rows s
    LEFT JOIN policies p ON p.id = s.policy_id
),
latest AS (
    -- Keep the earliest row per status, policy_holder_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claimed_amount_adj,
        claim_status_count,
        total_claim_claimed_amount,
        policy_holder_id,
        policy_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, policy_holder_id ORDER BY claimed_amount, id) AS rn
        FROM with_details_policy
    ) ranked
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, claimed_amount_adj, total_claim_claimed_amount
    FROM latest
) src
UNPIVOT (
    value FOR measure IN (claimed_amount_adj, total_claim_claimed_amount)
) unp
ORDER BY id, measure
