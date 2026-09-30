WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LENGTH(TRIM(status)) AS status_len,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM policies
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_end_date
    FROM prepared s
),
with_parent AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.premium_bucket,
        p.holder_name,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM with_stats s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE UPPER(TRIM(p.holder_name)) LIKE 'B%'
)
SELECT
    product,
    holder_segment,
    holder_region,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids
FROM with_parent
GROUP BY GROUPING SETS ((product, holder_segment, holder_region), (product), ())
ORDER BY product NULLS LAST, holder_segment NULLS LAST, holder_region NULLS LAST
