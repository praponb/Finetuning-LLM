WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        ADD_MONTHS(end_date, 3) AS end_date_plus_3m
    FROM policies
    WHERE product = 'HOME'
      AND status = 'ACTIVE'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date_plus_3m,
        c.id AS claim_id,
        c.claim_date,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM prepared_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    holder_id,
    claim_id,
    claim_type,
    MAX(claim_approved_amount) AS max_claim_approved_amount
FROM with_children
GROUP BY ROLLUP(holder_id, claim_id, claim_type)
ORDER BY holder_id NULLS LAST, claim_id NULLS LAST, claim_type NULLS LAST
