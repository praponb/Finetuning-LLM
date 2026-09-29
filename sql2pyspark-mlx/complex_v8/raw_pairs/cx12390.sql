WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE product NOT IN ('LIFE', 'TRAVEL')
      AND start_date >= DATE '2025-01-05'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        SUBSTRING(TRIM(product_prefix), 1, 4) AS product_prefix_prefix,
        ROUND(premium / NULLIF(premium_filled, 0), 3) AS premium_per_premium_filled
    FROM prepared_policys
    WHERE product_prefix IS NOT NULL
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.product_prefix,
        s.premium_filled,
        s.product_prefix_prefix,
        s.premium_per_premium_filled,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        p.holder_name
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    holder_id,
    holder_segment,
    ARRAY_SORT(COLLECT_SET(holder_name)) AS holder_name_list
FROM joined_holder
GROUP BY ROLLUP(holder_id, holder_segment)
ORDER BY holder_id NULLS LAST, holder_segment NULLS LAST
