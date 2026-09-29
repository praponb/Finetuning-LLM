WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter,
        ROUND(premium * 0.25, 2) AS premium_adj,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
      AND end_date < DATE '2026-09-19'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.start_date_quarter,
        c.id AS claim_id,
        c.claim_type,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount
    FROM base_policys s
    JOIN claims c ON c.policy_id = s.id
),
with_details_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.start_date_quarter,
        s.claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type,
        c.claimed_amount AS claim_claimed_amount_2
    FROM detail_rows s
    JOIN claims c ON c.id = s.claim_id
)
SELECT
    claim_adjuster_id,
    claim_claim_type,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(claim_claimed_amount_2), 2) AS stddev_claim_claimed_amount_2,
    COUNT(*) AS row_count
FROM with_details_claim
GROUP BY GROUPING SETS ((claim_adjuster_id, claim_claim_type), (claim_adjuster_id))
ORDER BY claim_adjuster_id NULLS LAST, claim_claim_type NULLS LAST
