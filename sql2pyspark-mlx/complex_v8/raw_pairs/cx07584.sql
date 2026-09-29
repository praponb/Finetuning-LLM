WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
    WHERE product IN ('HEALTH', 'LIFE', 'TRAVEL')
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count, MAX(premium) AS max_policy_premium
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        p.max_policy_premium
    FROM prepared s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_start_date,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_2,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_end_date
    FROM profiled s
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(premium_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', last_policy_end_date), policy_count_2 ORDER BY end_date_quarter DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_adj_avg_3,
        SIZE(COLLECT_SET(holder_id) OVER w_status) AS distinct_holder_id_in_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_policy_end_date), policy_count_2 ORDER BY end_date_quarter DESC), 3) AS end_date_quarter_percent_rank,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER w_status, 0), 4) AS premium_adj_share_of_status,
        LEAD(policy_count, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', last_policy_end_date), policy_count_2 ORDER BY end_date_quarter DESC, id) AS next3_policy_count,
        FIRST_VALUE(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', last_policy_end_date), policy_count_2 ORDER BY end_date_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date
    FROM with_counts
    WINDOW w_status AS (PARTITION BY status)
)
-- Subtotals with GROUPING SETS
SELECT
    policy_count,
    status,
    product,
    COUNT(*) FILTER (WHERE status = 'EXPIRED') AS n_status_expired,
    ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null
FROM windowed
GROUP BY GROUPING SETS ((policy_count, status, product), (policy_count))
ORDER BY policy_count NULLS LAST, status NULLS LAST, product NULLS LAST
