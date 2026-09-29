WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        MONTH(start_date) AS start_date_month,
        LENGTH(TRIM(product)) AS product_len,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM policies
    WHERE product <> 'HOME'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.product_prefix,
        c.id AS claim_id,
        c.claim_type,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date
    FROM cleaned s
    JOIN claims c ON c.policy_id = s.id
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM with_children s
    JOIN policyholders p ON p.id = s.holder_id AND p.risk_score >= 0.2
)
-- Subtotals with GROUPING SETS
SELECT
    product,
    holder_region,
    claim_type,
    GROUPING_ID() AS gid,
    MIN(start_date) AS first_start_date,
    MIN(claim_approved_amount) AS min_claim_approved_amount
FROM joined
GROUP BY GROUPING SETS ((product, holder_region, claim_type), (product), ())
