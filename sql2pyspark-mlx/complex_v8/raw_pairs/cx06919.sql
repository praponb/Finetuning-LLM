WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        UPPER(TRIM(product)) AS product_clean
    FROM policies
    WHERE (holder_id IS NOT NULL OR start_date < DATE '2025-05-28')
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, MIN(start_date) AS first_policy_start_date
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_start_date
    FROM filtered s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
filtered_2 AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.holder_name IS NOT NULL
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        end_date,
        status,
        status_group,
        days_end_date_to_start_date,
        product_clean,
        first_policy_start_date,
        LAG(days_end_date_to_start_date) OVER (PARTITION BY MONTH(start_date), DATE_TRUNC('MONTH', first_policy_start_date) ORDER BY start_date, id) AS prev_days_end_date_to_start_date,
        COUNT(*) OVER (PARTITION BY status_group, holder_id) AS rows_in_status_group_holder_id,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date), DATE_TRUNC('MONTH', first_policy_start_date) ORDER BY start_date, id) AS seq_no,
        SIZE(COLLECT_SET(status) OVER w_status_group_holder_id) AS distinct_status_in_status_group_holder_id,
        SUM(premium) OVER (ORDER BY days_end_date_to_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium
    FROM filtered_2
    WINDOW w_status_group_holder_id AS (PARTITION BY status_group, holder_id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        end_date,
        status,
        product_clean,
        first_policy_start_date,
        prev_days_end_date_to_start_date,
        rows_in_status_group_holder_id,
        seq_no,
        distinct_status_in_status_group_holder_id,
        running_premium,
        COUNT(*) OVER (ORDER BY running_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(running_premium) OVER (PARTITION BY status_group) AS status_group_sum_running_premium,
        FIRST_VALUE(first_policy_start_date) OVER (PARTITION BY seq_no, YEAR(first_policy_start_date) ORDER BY days_end_date_to_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_policy_start_date,
        NTH_VALUE(first_policy_start_date, 2) OVER (ORDER BY running_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_policy_start_date
    FROM ranked
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(first_first_policy_start_date, 'yyyy-MM') AS first_first_policy_start_date_ym,
        DATE_TRUNC('MONTH', second_first_policy_start_date) AS second_first_policy_start_date_month,
        SUM(CASE WHEN status IN ('CANCELLED', 'LAPSED', 'ACTIVE') THEN prev_days_end_date_to_start_date ELSE 0 END) AS prev_days_end_date_to_start_date_status_in_cance,
        ROUND(STDDEV(status_group_sum_running_premium), 2) AS stddev_status_group_sum_running_premium
    FROM metrics
    GROUP BY DATE_FORMAT(first_first_policy_start_date, 'yyyy-MM'), DATE_TRUNC('MONTH', second_first_policy_start_date)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        first_first_policy_start_date_ym,
        second_first_policy_start_date_month,
        prev_days_end_date_to_start_date_status_in_cance,
        ROUND(CUME_DIST() OVER w_second_first_policy_start_da, 3) AS prev_days_end_date_to_start_date_status_in_cance_2
    FROM per_group
    WINDOW w_second_first_policy_start_da AS (PARTITION BY TO_DATE(second_first_policy_start_date_month) ORDER BY prev_days_end_date_to_start_date_status_in_cance DESC, first_first_policy_start_date_ym, second_first_policy_start_date_month)
)
SELECT first_first_policy_start_date_ym
FROM sequenced
