WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        LENGTH(TRIM(product)) AS product_len,
        CASE WHEN product IN ('HOME', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE holder_id <= 13
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        product_len,
        product_group,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        FLOOR(product_len / 2) * 2 AS product_len_bucket,
        CONCAT(product_group, '#', CAST(id AS STRING)) AS product_group_ref
    FROM prepared
    WHERE product_len <= 9
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.product_len,
        s.product_prefix,
        s.product_group_ref,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM labeled s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    end_date,
    product_prefix,
    holder_risk_score,
    holder_region,
    DATE_TRUNC('WEEK', end_date) AS end_date_week,
    LOWER(TRIM(holder_region)) AS holder_region_clean
FROM combined_details_holder
ORDER BY holder_region NULLS LAST, holder_region_clean DESC
