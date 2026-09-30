WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE product NOT IN ('HOME', 'AUTO')
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        holder_segment,
        holder_id,
        COUNT(*) FILTER (WHERE holder_segment IN ('VIP', 'RETAIL')) AS n_holder_segment_in_vip_retail,
        COUNT(*) AS row_count,
        MAX(end_date) AS last_end_date
    FROM joined_holder
    WHERE holder_id IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', end_date), holder_segment, holder_id
),
detail_rows AS (
    -- One row per policy
    SELECT
        s.end_date_month,
        s.holder_segment,
        s.holder_id,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM per_group s
    JOIN policies p ON p.holder_id = s.holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM detail_rows s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT
    policy_id,
    holder_id,
    GROUPING_ID() AS gid,
    PERCENTILE_APPROX(policy_count, 0.5) AS median_policy_count
FROM summarized_rows
GROUP BY ROLLUP(policy_id, holder_id)
