WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 8 AND 12
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM recent_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.premium >= 2096.6
              AND p.product = 'TRAVEL'
        )
)
SELECT id, field, value
FROM (
    SELECT id, claimed_amount_minus_approved_amount, least_approved_amount_claimed_amount, claimed_amount
    FROM filtered
) src
UNPIVOT (
    value FOR field IN (claimed_amount_minus_approved_amount, least_approved_amount_claimed_amount, claimed_amount)
) unp
ORDER BY id, field
