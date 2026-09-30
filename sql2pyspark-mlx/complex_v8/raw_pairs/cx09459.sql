WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE (status IS NOT NULL OR adjuster_id >= 6)
)
SELECT policy_id, claim_type AS label, reported_at
FROM claims
WHERE claimed_amount <= 40638.4
UNION ALL
SELECT id AS policy_id, claim_type AS label, reported_at
FROM claims
WHERE status = 'PAID'
UNION ALL
SELECT adjuster_id AS policy_id, status AS label, reported_at
FROM claims
