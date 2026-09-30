WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        AVG(cost) OVER (ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        DENSE_RANK() OVER w_running AS cost_dense_rank
    FROM shipments
    WINDOW w_running AS (PARTITION BY DATE_TRUNC('week', shipped_date), carrier ORDER BY cost DESC NULLS LAST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        delivered_date,
        status,
        cumulative_cost,
        cost_avg_6,
        cost_dense_rank,
        LAST_DAY(delivered_date) AS delivered_date_month_end,
        LENGTH(status) AS status_length
    FROM scored
    WHERE delivered_date > TO_DATE('2023-03-31')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_length) OVER w_running AS running_sum_status_length,
        COUNT(status_length) OVER w_running AS running_count_status_length,
        MIN(status_length) OVER w_running AS running_min_status_length,
        MAX(status_length) OVER w_running AS running_max_status_length,
        LAST_VALUE(cost_dense_rank, TRUE) OVER w_running AS last_known_cost_dense_rank
    FROM valid
    WINDOW w_running AS (PARTITION BY status ORDER BY cumulative_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(cumulative_cost) IGNORE NULLS OVER (PARTITION BY status ORDER BY running_min_status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_cost,
        LAG(status_length, 2) OVER w_part AS prev_status_length
    FROM enriched
    WINDOW w_part AS (ORDER BY cost_avg_6, id)
)
-- Aggregate with conditional metrics
SELECT
    status,
    DATE_TRUNC('QUARTER', delivered_date_month_end) AS quarter_start,
    CASE WHEN running_sum_status_length >= 340 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_status_length_tier,
    PERCENTILE_APPROX(running_sum_status_length, 0.95) AS running_sum_status_length_p95,
    PERCENTILE_APPROX(cost_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS cost_dense_rank_quartiles,
    COUNT_IF(status = 'LOST') AS lost_events,
    ROUND(AVG(cumulative_cost), 2) AS avg_cumulative_cost,
    ROUND(SUM(first_known_cumulative_cost) / NULLIF(COUNT(CASE WHEN status = 'LOST' THEN 1 END), 0), 2) AS first_known_cumulative_cost_per_lost,
    MAX(last_known_cost_dense_rank) FILTER (WHERE status = 'RETURNED') AS returned_max_last_known_cost_dense_rank
FROM scored_shipments
GROUP BY GROUPING SETS ((status, DATE_TRUNC('QUARTER', delivered_date_month_end), CASE WHEN running_sum_status_length >= 340 THEN 'PREMIUM' ELSE 'STANDARD' END), (status, DATE_TRUNC('QUARTER', delivered_date_month_end)), (status));
