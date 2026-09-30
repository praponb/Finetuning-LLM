WITH cleaned AS (
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
        reported_at,
        ABS(approved_amount - 9389.3) AS approved_amount_distance,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
    FROM claims
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.days_since_reported_at,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM cleaned s
    JOIN claims c ON c.policy_id = s.policy_id
),
unique_rows AS (
    -- Keep the earliest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        days_since_reported_at,
        claim_id,
        claim_reported_at,
        claim_claim_date,
        claim_claimed_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claim_claimed_amount, id, claim_id) AS recency_rank
        FROM with_children
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, field, value
FROM (
    SELECT id, claim_id, claimed_amount, claim_claimed_amount, approved_amount
    FROM unique_rows
) src
UNPIVOT (
    value FOR field IN (claimed_amount, claim_claimed_amount, approved_amount)
) unp
