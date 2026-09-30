WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        ROUND(premium * 1.5, 2) AS premium_adj
    FROM policies
    WHERE product = 'TRAVEL'
      AND premium > 1042.5
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        p.joined_on AS holder_joined_on
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        holder_joined_on,
        LAG(holder_joined_on) OVER (PARTITION BY status ORDER BY holder_joined_on ASC, id) AS prev_holder_joined_on,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY holder_joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date
    FROM with_parent
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.holder_joined_on,
        s.first_known_end_date,
        p.region AS holder_region
    FROM metrics s
    JOIN policyholders p ON p.id = s.holder_id
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.holder_joined_on,
        s.holder_region,
        p.region AS holder_region_2,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM with_parent_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
status_stats AS (
    -- Max holder_risk_score per status
    SELECT status, MAX(holder_risk_score) AS max_holder_risk_score
    FROM combined_details
    GROUP BY status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_holder_risk_score,
        ROUND(s.holder_risk_score - g.max_holder_risk_score, 2) AS holder_risk_score_minus_max
    FROM combined_details s
    JOIN status_stats g ON g.status = s.status
    WHERE s.holder_risk_score = g.max_holder_risk_score
),
rollup_base AS (
    -- Summarise per group
    SELECT
        holder_id,
        status,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        ARRAY_SORT(COLLECT_SET(holder_segment)) AS holder_segment_list,
        ROUND(100 * COUNT(CASE WHEN holder_region IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_holder_region_is_not_null
    FROM vs_group
    GROUP BY 1, 2
)
-- Final projection
SELECT
    holder_id,
    status,
    status_list,
    pct_holder_region_is_not_null,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM rollup_base
ORDER BY status DESC
