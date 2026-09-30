WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        ROUND(premium * 1.5, 2) AS premium_adj,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE product NOT IN ('AUTO', 'LIFE')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.days_start_date_to_end_date,
        s.product_prefix,
        p.holder_name
    FROM scoped s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.claim_date
    FROM with_parent s
    LEFT JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    status,
    claim_id,
    claim_claimed_amount,
    CASE WHEN claim_claimed_amount >= 34763.4 THEN 'A' WHEN claim_claimed_amount >= 18364.1 THEN 'B' ELSE 'C' END AS claim_claimed_amount_tier
FROM line_level
WHERE claim_id IS NOT NULL
ORDER BY id DESC, claim_claimed_amount_tier
