WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product IS NOT NULL
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        status,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        SUM(premium) FILTER (WHERE status NOT IN ('LAPSED', 'CANCELLED')) AS premium_status_not_in_lapsed_cancell,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        COUNT(DISTINCT product) AS distinct_products,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM scoped
    WHERE holder_id >= 16
    GROUP BY holder_id, status
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Derive helper columns
    SELECT
        holder_id,
        status,
        premium_status_not_in_lapsed_cancell,
        product_list,
        distinct_products,
        status_list,
        FLOOR(distinct_products / 2) * 2 AS distinct_products_bucket,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM rollup_base
    WHERE SIZE(status_list) >= 1
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM labeled s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment = 'RETAIL'
),
per_label AS (
    -- One row per status_list_item
    SELECT
        s.holder_id,
        s.status,
        s.premium_status_not_in_lapsed_cancell,
        s.product_list,
        s.distinct_products,
        s.distinct_products_bucket,
        s.status_group,
        s.status_prefix,
        s.holder_risk_score,
        s.holder_joined_on,
        s.holder_segment,
        t.status_list_item
    FROM with_parent_holder s
    LATERAL VIEW OUTER EXPLODE(s.status_list) t AS status_list_item
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_products, distinct_products_bucket) AS rows_in_distinct_products_distinct_products_buck,
        MIN(distinct_products_bucket) OVER (PARTITION BY distinct_products, distinct_products_bucket) AS distinct_products_distinct_products_bucket_min_d,
        ROUND(distinct_products - AVG(distinct_products) OVER (PARTITION BY distinct_products, distinct_products_bucket), 2) AS distinct_products_vs_distinct_products_distinct_,
        ROUND(distinct_products_bucket / NULLIF(SUM(distinct_products_bucket) OVER (PARTITION BY distinct_products, distinct_products_bucket), 0), 4) AS distinct_products_bucket_share_of_distinct_produ,
        ROUND(distinct_products_bucket - AVG(distinct_products_bucket) OVER (PARTITION BY distinct_products, distinct_products_bucket), 2) AS distinct_products_bucket_vs_distinct_products_di,
        SUM(distinct_products_bucket) OVER (PARTITION BY distinct_products, distinct_products_bucket) AS distinct_products_distinct_products_bucket_sum_d
    FROM per_label
)
SELECT
    holder_id,
    premium_status_not_in_lapsed_cancell,
    distinct_products,
    status_prefix,
    holder_risk_score,
    holder_joined_on,
    holder_segment,
    rows_in_distinct_products_distinct_products_buck,
    distinct_products_distinct_products_bucket_min_d,
    distinct_products_bucket_share_of_distinct_produ,
    distinct_products_bucket_vs_distinct_products_di,
    distinct_products_distinct_products_bucket_sum_d,
    COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown
FROM with_windows
ORDER BY holder_segment_or_unknown NULLS LAST, distinct_products_distinct_products_bucket_sum_d DESC
