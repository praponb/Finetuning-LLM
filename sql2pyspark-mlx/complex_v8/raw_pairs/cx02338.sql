WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        CASE WHEN product IN ('AUTO', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
),
latest_per_key AS (
    -- Keep the latest row per product_group
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        product_prefix,
        product_group,
        status_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY start_date DESC, id DESC) AS rn
        FROM prepared_policys
    ) ranked
    WHERE rn = 1
),
joined_holder AS (
    -- Join policyholders
    SELECT s.*, p.segment AS holder_segment, p.holder_name
    FROM latest_per_key s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status,
    product_prefix,
    status_ref,
    holder_segment,
    FLOOR(premium / 1000) * 1000 AS premium_bucket
FROM joined_holder
ORDER BY end_date
