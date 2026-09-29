WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id >= 18
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT s.*, p.risk_score AS holder_risk_score, p.joined_on AS holder_joined_on
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        s.holder_risk_score,
        s.holder_joined_on,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on_2
    FROM enriched_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    COALESCE(holder_region, 'ALL') AS holder_region,
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    ROUND(SUM(holder_risk_score), 2) AS total_holder_risk_score
FROM combined_details_holder
GROUP BY CUBE(holder_region, product, status)
ORDER BY holder_region NULLS LAST, product NULLS LAST, status NULLS LAST
