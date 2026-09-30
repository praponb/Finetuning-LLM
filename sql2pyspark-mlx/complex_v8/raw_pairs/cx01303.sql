WITH recent AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE premium >= 2013.8
),
annotated AS (
    -- Derive helper columns
    SELECT *, CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM recent
    WHERE premium >= 1051.9
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        p.segment AS holder_segment
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown,
        CASE WHEN product IN ('LIFE', 'TRAVEL') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
    FROM with_details_holder
)
-- Subtotals with ROLLUP
SELECT
    product,
    holder_segment,
    GROUPING_ID() AS gid,
    MAX(premium) AS max_premium,
    COUNT(*) AS n_rows
FROM tagged
GROUP BY ROLLUP(product, holder_segment)
ORDER BY product NULLS LAST, holder_segment NULLS LAST
