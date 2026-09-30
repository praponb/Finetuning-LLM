WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        CONCAT_WS(' / ', product, status) AS product_status_label,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE status = 'LAPSED'
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
        )
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.premium_filled,
        s.product_status_label,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM qualified s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    premium_filled,
    product_status_label,
    holder_segment,
    holder_joined_on
FROM with_details_holder
ORDER BY premium DESC, premium_filled NULLS LAST
