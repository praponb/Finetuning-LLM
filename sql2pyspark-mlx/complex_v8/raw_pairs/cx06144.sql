WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM policies
    WHERE end_date BETWEEN DATE '2024-11-08' AND DATE '2026-10-23'
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.days_since_end_date,
        s.product_prefix,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.days_since_end_date,
        s.product_prefix,
        s.holder_risk_score,
        s.holder_region,
        s.holder_segment,
        p.joined_on AS holder_joined_on
    FROM combined_details s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    end_date,
    days_since_end_date,
    product_prefix,
    holder_segment,
    holder_joined_on,
    YEAR(holder_joined_on) AS holder_joined_on_year
FROM with_parent_holder
WHERE holder_region IN ('Central', 'North')
ORDER BY days_since_end_date, end_date DESC
