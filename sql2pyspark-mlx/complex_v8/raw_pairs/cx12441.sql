WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('TRAVEL', 'HOME') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE holder_id >= 6
      AND product IN ('AUTO', 'HOME', 'HEALTH')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(product_group), 1, 3) AS product_group_prefix,
        MONTH(start_date) AS start_date_month
    FROM filtered
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.product_group,
        s.start_date_month,
        p.risk_score AS holder_risk_score
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score <= 0.6
)
-- Subtotals with GROUPING SETS
SELECT
    product,
    holder_id,
    start_date_month,
    GROUPING_ID() AS gid,
    MAX(premium) AS max_premium
FROM combined_details
GROUP BY GROUPING SETS ((product, holder_id, start_date_month), (product))
ORDER BY product NULLS LAST, holder_id NULLS LAST, start_date_month NULLS LAST
