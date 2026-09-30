WITH base AS (
    -- Scope the claims rows
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
    WHERE claimed_amount > 19347.2
      AND policy_id >= 8
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        p.start_date AS policy_start_date
    FROM base s
    LEFT JOIN policies p ON p.id = s.policy_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount
    FROM combined_details_policy s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.policy_id IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_policy_id, claim_type, approved_amount
    FROM line_level
) src
PIVOT (
    COUNT(approved_amount) AS count_v, MAX(approved_amount) AS max_v
    FOR claim_type IN ('medical', 'delay', 'collision')
)
ORDER BY claim_policy_id
