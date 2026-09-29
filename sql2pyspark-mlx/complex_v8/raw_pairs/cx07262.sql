WITH filtered_holders AS (
    -- Filter policyholders
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        MONTH(joined_on) AS joined_on_month,
        LOWER(REGEXP_REPLACE(TRIM(segment), ' ', '_')) AS segment_slug
    FROM policyholders
    WHERE risk_score < 0.7
      AND joined_on >= DATE '2024-09-04'
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.product IN ('HOME', 'TRAVEL', 'HEALTH')) FROM policies p WHERE p.holder_id = s.id) AS policy_product_count
    FROM filtered_holders s
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.id) AS first_policy_start_date
    FROM summarized_rows s
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(first_policy_start_date, 'yyyy-MM') AS first_policy_start_date_ym,
        policy_count,
        policy_product_count,
        PERCENTILE_APPROX(policy_product_count, 0.5) AS median_policy_product_count,
        SUM(policy_count) AS total_policy_count
    FROM profiled
    GROUP BY DATE_FORMAT(first_policy_start_date, 'yyyy-MM'), policy_count, policy_product_count
),
ranked AS (
    -- Previous / next values per key
    SELECT
        first_policy_start_date_ym,
        policy_count,
        policy_product_count,
        median_policy_product_count,
        total_policy_count,
        COUNT(*) OVER (PARTITION BY policy_product_count, median_policy_product_count) AS rows_in_policy_product_count_median_policy_produ,
        NTH_VALUE(policy_product_count, 2) OVER (PARTITION BY median_policy_product_count, policy_product_count ORDER BY total_policy_count ASC, first_policy_start_date_ym, policy_count, policy_product_count ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_product_count,
        total_policy_count - LAG(total_policy_count) OVER (PARTITION BY median_policy_product_count, policy_product_count ORDER BY total_policy_count ASC, first_policy_start_date_ym, policy_count, policy_product_count) AS total_policy_count_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY median_policy_product_count, policy_product_count ORDER BY total_policy_count ASC, first_policy_start_date_ym, policy_count, policy_product_count), 3) AS total_policy_count_percent_rank,
        LEAD(policy_count, 3) OVER w_median_policy_product_count_2 AS next3_policy_count,
        SUM(total_policy_count) OVER (PARTITION BY median_policy_product_count, policy_product_count ORDER BY total_policy_count ASC, first_policy_start_date_ym, policy_count, policy_product_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_policy_count
    FROM stats
    WINDOW w_median_policy_product_count_2 AS (PARTITION BY median_policy_product_count, policy_product_count ORDER BY total_policy_count ASC, first_policy_start_date_ym, policy_count, policy_product_count)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(running_total_policy_count) OVER (PARTITION BY policy_count, next3_policy_count) AS policy_count_next3_policy_count_sum_running_tota,
        MIN(policy_product_count) OVER (PARTITION BY policy_count, next3_policy_count) AS policy_count_next3_policy_count_min_policy_produ,
        ROUND(AVG(running_total_policy_count) OVER (PARTITION BY policy_count, next3_policy_count), 2) AS policy_count_next3_policy_count_avg_running_tota,
        ROUND(next3_policy_count - AVG(next3_policy_count) OVER (PARTITION BY policy_count, next3_policy_count), 2) AS next3_policy_count_vs_policy_count_next3_policy_
    FROM ranked
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(next3_policy_count, 0) AS next3_policy_count_filled,
        next3_policy_count - policy_product_count AS next3_policy_count_minus_policy_product_count
    FROM with_windows
)
SELECT
    first_policy_start_date_ym,
    policy_count,
    policy_product_count,
    total_policy_count,
    rows_in_policy_product_count_median_policy_produ,
    total_policy_count_change,
    total_policy_count_percent_rank,
    next3_policy_count,
    policy_count_next3_policy_count_min_policy_produ,
    policy_count_next3_policy_count_avg_running_tota,
    next3_policy_count_filled,
    next3_policy_count_minus_policy_product_count,
    GREATEST(policy_product_count, next3_policy_count) AS greatest_policy_product_count_next3_policy_count
FROM labeled
ORDER BY policy_count_next3_policy_count_avg_running_tota
