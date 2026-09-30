WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE status = 'LAPSED'
      AND end_date >= DATE '2025-10-12'
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT s.*, p.risk_score AS holder_risk_score
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score > 0.1
)
SELECT *
FROM (
    SELECT end_date, holder_id, status, premium_bucket
    FROM enriched_holder
) src
PIVOT (
    MIN(premium_bucket)
    FOR status IN ('ACTIVE', 'LAPSED')
)
ORDER BY end_date, holder_id
