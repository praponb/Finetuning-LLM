WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
    WHERE end_date BETWEEN DATE '2025-10-12' AND DATE '2026-09-23'
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT s.*, p.joined_on AS holder_joined_on, p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2025-04-20'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(premium_adj) OVER (PARTITION BY status ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_adj,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id) AS rn,
        LEAD(start_date) OVER (PARTITION BY status ORDER BY premium, id) AS next_start_date,
        MAX(premium_adj) OVER (PARTITION BY status) AS status_max_premium_adj,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        DENSE_RANK() OVER w_holder_id_by_holder_joined_o AS holder_joined_on_dense_rank,
        premium_adj - LAG(premium_adj) OVER w_holder_segment_by_start_date AS premium_adj_change,
        SIZE(COLLECT_SET(holder_id) OVER w_status) AS distinct_holder_id_in_status,
        MAX(premium_adj) OVER w_holder_segment AS holder_segment_max_premium_adj,
        LEAD(premium_adj, 3) OVER (PARTITION BY status ORDER BY start_date DESC, id) AS next3_premium_adj,
        LAST_VALUE(holder_joined_on) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_joined_on,
        SIZE(COLLECT_SET(holder_id) OVER w_holder_segment) AS distinct_holder_id_in_holder_segment,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg
    FROM combined_details_holder
    WINDOW w_holder_id_by_holder_joined_o AS (PARTITION BY holder_id ORDER BY holder_joined_on ASC NULLS LAST), w_holder_segment_by_start_date AS (PARTITION BY holder_segment ORDER BY start_date, id), w_status AS (PARTITION BY status), w_holder_segment AS (PARTITION BY holder_segment)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        start_date_label,
        premium_adj,
        holder_joined_on,
        holder_segment,
        running_premium_adj,
        rn,
        next_start_date,
        status_max_premium_adj,
        premium_share_of_status,
        premium_adj_change,
        distinct_holder_id_in_status,
        holder_segment_max_premium_adj,
        next3_premium_adj,
        last_known_holder_joined_on,
        distinct_holder_id_in_holder_segment,
        premium_vs_status_avg
    FROM metrics
    WHERE holder_joined_on_dense_rank = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        holder_segment,
        DATE_TRUNC('MONTH', next_start_date) AS next_start_date_month,
        SUM(premium_share_of_status) AS total_premium_share_of_status,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN holder_segment <> 'RETAIL' THEN 1 ELSE 0 END) AS n_holder_segment_retail
    FROM first_rows
    GROUP BY 1, 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(n_rows - AVG(n_rows) OVER (PARTITION BY holder_segment), 2) AS n_rows_vs_holder_segment_avg,
        ROUND(PERCENT_RANK() OVER w_next_start_date_month_month, 3) AS n_holder_segment_retail_percent_rank,
        FIRST_VALUE(n_rows) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', next_start_date_month) ORDER BY n_holder_segment_retail, holder_segment, next_start_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_rows
    FROM rollup_base
    WINDOW w_next_start_date_month_month AS (PARTITION BY DATE_TRUNC('MONTH', next_start_date_month) ORDER BY n_holder_segment_retail, holder_segment, next_start_date_month)
)
SELECT
    holder_segment,
    next_start_date_month,
    total_premium_share_of_status,
    n_rows_vs_holder_segment_avg,
    first_known_n_rows,
    LEAST(first_known_n_rows, n_rows) AS least_first_known_n_rows_n_rows
FROM ranked
ORDER BY least_first_known_n_rows_n_rows DESC NULLS FIRST, next_start_date_month NULLS LAST
