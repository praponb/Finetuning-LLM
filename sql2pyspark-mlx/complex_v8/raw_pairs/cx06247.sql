WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-07-07'
      AND end_date BETWEEN DATE '2025-12-05' AND DATE '2026-09-19'
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IN ('flood', 'fire')
              AND c.adjuster_id IS NOT NULL
        )
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.days_end_date_to_start_date,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM with_activity s
    JOIN claims c ON c.policy_id = s.id
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.days_end_date_to_start_date,
        s.claim_id,
        s.claim_approved_amount,
        c.id AS claim_id_2,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount_2,
        c.adjuster_id AS claim_adjuster_id
    FROM line_level_claims s
    JOIN claims c ON c.policy_id = s.id AND c.approved_amount >= 0.0
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        product,
        days_end_date_to_start_date,
        claim_id,
        claim_approved_amount,
        claim_id_2,
        claim_claimed_amount,
        claim_approved_amount_2,
        claim_adjuster_id,
        claim_approved_amount - claim_claimed_amount AS claim_approved_amount_minus_claim_claimed_amount,
        claim_approved_amount + claim_claimed_amount AS claim_approved_amount_plus_claim_claimed_amount
    FROM detail_rows_claims
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_id, claim_id_2, product, claim_approved_amount_plus_claim_claimed_amount
    FROM scored
) src
PIVOT (
    SUM(claim_approved_amount_plus_claim_claimed_amount)
    FOR product IN ('LIFE', 'TRAVEL', 'HEALTH', 'AUTO')
)
ORDER BY claim_id, claim_id_2
