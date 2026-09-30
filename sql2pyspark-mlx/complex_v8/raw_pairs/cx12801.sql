WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE (status IN ('CANCELLED', 'ACTIVE') OR product = 'HOME')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(premium, 0.0) AS premium_filled,
        UPPER(TRIM(status)) AS status_clean,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM cleaned
    WHERE holder_id >= 6
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.premium_filled,
        s.status_clean,
        s.product_prefix,
        p.risk_score AS holder_risk_score
    FROM enriched s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(product, 'ALL') AS product,
    holder_id,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids,
    SUM(premium) AS total_premium
FROM with_details_holder
GROUP BY CUBE(product, holder_id)
