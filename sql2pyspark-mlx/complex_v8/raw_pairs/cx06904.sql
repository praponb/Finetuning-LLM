WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product = 'AUTO'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        start_date_plus_12m,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY holder_id), 2) AS days_since_start_date_vs_holder_id_avg,
        SIZE(COLLECT_SET(holder_id) OVER w_start_date_month_status) AS distinct_holder_id_in_start_date_month_status,
        FIRST_VALUE(start_date_plus_12m) OVER (PARTITION BY status, holder_id ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date_plus_12m,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY status, holder_id ORDER BY start_date ASC, id)) AS days_since_prev_start_date,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        SUM(days_since_start_date) OVER (ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_start_date_sum_2_prior,
        ROUND(CUME_DIST() OVER w_all_by_start_date, 3) AS start_date_cume_dist,
        MAX(days_since_start_date) OVER (PARTITION BY status, holder_id ORDER BY start_date ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_start_date_max_6_prior,
        ROUND(days_since_start_date / NULLIF(SUM(days_since_start_date) OVER (PARTITION BY MONTH(start_date), status), 0), 4) AS days_since_start_date_share_of_start_date_month_,
        LAG(days_since_start_date) OVER (ORDER BY start_date DESC, id) AS prev_days_since_start_date,
        MIN(days_since_start_date) OVER (PARTITION BY YEAR(end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_start_date_so_far,
        SUM(days_since_start_date) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_start_date_sum_6_prior,
        SUM(days_since_start_date) OVER (PARTITION BY MONTH(start_date), status) AS start_date_month_status_sum_days_since_start_dat,
        ROUND(days_since_start_date / NULLIF(SUM(days_since_start_date) OVER (PARTITION BY holder_id), 0), 4) AS days_since_start_date_share_of_holder_id
    FROM valid_policys
    WINDOW w_start_date_month_status AS (PARTITION BY MONTH(start_date), status), w_holder_id AS (PARTITION BY holder_id), w_all_by_start_date AS (ORDER BY start_date, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        end_date,
        status,
        start_date_plus_12m,
        distinct_holder_id_in_start_date_month_status,
        days_since_start_date_sum_2_prior,
        start_date_cume_dist,
        days_since_start_date_max_6_prior,
        min_days_since_start_date_so_far,
        days_since_start_date_share_of_holder_id,
        DATEDIFF(first_start_date_plus_12m, LAG(first_start_date_plus_12m) OVER (PARTITION BY MONTH(end_date) ORDER BY end_date DESC, id)) AS days_since_prev_first_start_date_plus_12m,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date ASC, id), 3) AS start_date_percent_rank,
        COUNT(*) OVER (PARTITION BY distinct_holder_id_in_start_date_month_status) AS rows_in_distinct_holder_id_in_start_date_month_s,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date_plus_12m), holder_id ORDER BY min_days_since_start_date_so_far, id) AS min_days_since_start_date_so_far_dense_rank,
        FIRST_VALUE(start_date_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date_plus_12m), holder_id ORDER BY min_days_since_start_date_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date_cume_dist,
        SUM(prev_days_since_start_date) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_days_since_start_date,
        ROUND(AVG(days_since_prev_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date_plus_12m), holder_id ORDER BY min_days_since_start_date_so_far, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS days_since_prev_start_date_avg_2_prior
    FROM ranked
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        distinct_holder_id_in_start_date_month_status,
        ROUND(SUM(first_start_date_cume_dist), 2) AS total_first_start_date_cume_dist,
        MAX(start_date_percent_rank) AS max_start_date_percent_rank,
        COUNT(*) AS cnt,
        MAX(end_date) AS last_end_date,
        ROUND(100 * COUNT(CASE WHEN status NOT IN ('CANCELLED', 'ACTIVE') THEN 1 END) / COUNT(*), 1) AS pct_status_not_in_cancelled_acti
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', end_date), distinct_holder_id_in_start_date_month_status
    HAVING COUNT(*) >= 2
)
SELECT DISTINCT cnt, end_date_month, max_start_date_percent_rank
FROM grouped
WHERE max_start_date_percent_rank <= 0.8
ORDER BY max_start_date_percent_rank
