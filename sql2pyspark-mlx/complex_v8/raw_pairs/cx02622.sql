WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE product = 'AUTO'
      AND premium >= 2033.8
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        holder_id,
        product,
        MAX(premium) AS max_premium,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) FILTER (WHERE product IN ('HOME', 'HEALTH')) AS n_product_in_home_health,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        MAX(status_len) AS max_status_len
    FROM base
    GROUP BY 1, 2, 3
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count, MIN(premium) AS min_policy_premium
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        p.min_policy_premium
    FROM grouped s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.end_date_month,
        s.holder_id,
        s.product,
        s.status_list,
        s.n_rows,
        s.distinct_holder_ids,
        s.policy_count,
        s.min_policy_premium,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM with_counts s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM enriched_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product NOT IN ('HEALTH', 'HOME')
              AND p.premium >= 1541.8
        )
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.end_date_month,
        s.holder_id,
        s.product,
        s.status_list,
        s.n_rows,
        s.distinct_holder_ids,
        s.policy_count,
        s.min_policy_premium,
        s.holder_joined_on,
        s.holder_segment,
        p.segment AS holder_segment_2
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id AND p.region IN ('East', 'West')
    WHERE p.segment <> 'SME'
)
-- Final projection
SELECT
    end_date_month,
    holder_id,
    status_list,
    n_rows,
    policy_count,
    holder_joined_on,
    holder_segment,
    holder_segment_2,
    YEAR(holder_joined_on) AS holder_joined_on_year
FROM with_parent_holder
ORDER BY holder_joined_on
