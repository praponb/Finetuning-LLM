WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE (premium > 1541.8 OR holder_id IS NOT NULL)
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.premium_filled,
        s.product_clean,
        p.risk_score AS holder_risk_score
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score < 0.6
)
SELECT id, measure, value
FROM (
    SELECT id, holder_risk_score, premium_filled
    FROM with_details_holder
) src
UNPIVOT (
    value FOR measure IN (holder_risk_score, premium_filled)
) unp
