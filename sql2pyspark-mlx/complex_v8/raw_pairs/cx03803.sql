WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        CONCAT_WS('-', product, status) AS product_status_label
    FROM policies
    WHERE (holder_id >= 8 OR start_date < DATE '2025-03-23')
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        s.product_or_unknown,
        s.product_slug,
        s.product_status_label,
        p.joined_on AS holder_joined_on
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id AND p.region = 'North'
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details_holder
    WHERE premium > (SELECT AVG(premium) FROM combined_details_holder)
),
combined_details_holder_2 AS (
    -- Join policyholders
    SELECT
        s.*,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on_2
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with ROLLUP
SELECT
    status,
    holder_region,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids,
    COUNT(DISTINCT product) AS distinct_products
FROM combined_details_holder_2
GROUP BY ROLLUP(status, holder_region)
ORDER BY status NULLS LAST, holder_region NULLS LAST
