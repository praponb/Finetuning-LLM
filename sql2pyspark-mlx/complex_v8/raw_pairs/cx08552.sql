WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE premium < 2844.1
      AND product IN ('HOME', 'HEALTH', 'LIFE')
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount
    FROM valid_policys s
    JOIN claims c ON c.policy_id = s.id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM line_level_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount >= 0.0
              AND c.claimed_amount BETWEEN 7204.1 AND 34763.4
        )
      AND s.claim_id >= 66
)
SELECT *
FROM (
    SELECT claim_reported_at, product, claim_approved_amount
    FROM screened
) src
PIVOT (
    MAX(claim_approved_amount)
    FOR product IN ('AUTO', 'HEALTH', 'TRAVEL')
)
ORDER BY claim_reported_at
