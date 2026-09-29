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
        COALESCE(premium, 0.0) AS premium_filled,
        LOWER(TRIM(product)) AS product_clean,
        CASE WHEN product IN ('HOME', 'HEALTH') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE YEAR(start_date) = 2025
      AND holder_id >= 5
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_filled,
        s.product_clean,
        s.product_group,
        p.segment AS holder_segment
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.region NOT IN ('North', 'East')
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.premium_filled,
        s.product_clean,
        s.holder_segment,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment_2
    FROM with_parent s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE LOWER(p.holder_name) LIKE '%rt%'
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    COALESCE(product, 'ALL') AS product,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(holder_segment_2)) AS holder_segment_2_list,
    MIN(premium_filled) AS min_premium_filled
FROM joined_holder
GROUP BY GROUPING SETS ((holder_id, product), (holder_id), ())
