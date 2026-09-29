WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        DATE_TRUNC('WEEK', start_date) AS start_date_week
    FROM policies
    WHERE premium >= 1042.5
      AND holder_id IS NOT NULL
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.product_ref,
        s.start_date_week,
        p.joined_on AS holder_joined_on
    FROM scoped s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        status,
        product_ref,
        holder_joined_on,
        RANK() OVER (PARTITION BY holder_id ORDER BY end_date NULLS LAST) AS end_date_rank,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id ORDER BY end_date, id)) AS days_since_prev_start_date,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_end_date_ties_2, 3) AS end_date_percent_rank,
        RANK() OVER w_start_date_month_status_by_h AS holder_joined_on_rank,
        ROW_NUMBER() OVER w_holder_id_by_end_date_2 AS seq_no,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY holder_joined_on ASC NULLS LAST, id)) AS days_since_prev_holder_joined_on,
        ROW_NUMBER() OVER w_start_date_month_status_by_h_2 AS rn,
        LEAD(end_date) OVER w_start_date_month_status_by_h_2 AS next_end_date,
        LEAD(start_date_week, 2) OVER (PARTITION BY holder_id ORDER BY end_date, id) AS next2_start_date_week,
        LAG(start_date_week, 2) OVER w_start_date_month_status_by_h_2 AS prev2_start_date_week,
        LAG(start_date, 3) OVER (PARTITION BY holder_id ORDER BY end_date, id) AS prev3_start_date
    FROM combined_details_holder
    WINDOW w_holder_id_by_end_date_ties_2 AS (PARTITION BY holder_id ORDER BY end_date), w_start_date_month_status_by_h AS (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY holder_joined_on ASC NULLS LAST), w_holder_id_by_end_date_2 AS (PARTITION BY holder_id ORDER BY end_date NULLS LAST, id), w_start_date_month_status_by_h_2 AS (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY holder_joined_on ASC NULLS LAST, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE rn <= 2
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        status,
        product_ref,
        holder_joined_on,
        end_date_rank,
        days_since_prev_start_date,
        end_date_percent_rank,
        holder_joined_on_rank,
        rn,
        next_end_date,
        next2_start_date_week,
        prev2_start_date_week,
        ROUND(CUME_DIST() OVER (PARTITION BY end_date_rank ORDER BY days_since_prev_holder_joined_on DESC), 3) AS days_since_prev_holder_joined_on_cume_dist,
        MIN(seq_no) OVER w_holder_joined_on_month_end_d AS holder_joined_on_month_end_date_month_min_seq_no,
        ROUND(AVG(days_since_prev_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), MONTH(end_date)), 2) AS holder_joined_on_month_end_date_month_avg_days_s,
        LEAD(next2_start_date_week, 3) OVER w_prev2_start_date_week_month AS next3_next2_start_date_week,
        ROUND((UNIX_TIMESTAMP(prev2_start_date_week) - UNIX_TIMESTAMP(LAG(prev2_start_date_week) OVER (PARTITION BY seq_no ORDER BY days_since_prev_start_date DESC, id))) / 60, 1) AS minutes_since_prev_prev2_start_date_week,
        ROUND(days_since_prev_start_date - AVG(days_since_prev_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), MONTH(end_date)), 2) AS days_since_prev_start_date_vs_holder_joined_on_m,
        SUM(days_since_prev_start_date) OVER (PARTITION BY YEAR(end_date), status) AS end_date_year_status_sum_days_since_prev_start_d,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER w_seq_no_by_days_since_prev_st) AS days_since_prev_holder_joined_on_2
    FROM top_rows
    WINDOW w_holder_joined_on_month_end_d AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), MONTH(end_date)), w_prev2_start_date_week_month AS (PARTITION BY MONTH(prev2_start_date_week) ORDER BY end_date DESC, id), w_seq_no_by_days_since_prev_st AS (PARTITION BY seq_no ORDER BY days_since_prev_start_date DESC, id)
)
-- Final projection
SELECT
    id,
    end_date,
    product_ref,
    holder_joined_on,
    days_since_prev_start_date,
    end_date_percent_rank,
    rn,
    next_end_date,
    next2_start_date_week,
    prev2_start_date_week,
    days_since_prev_holder_joined_on_cume_dist,
    holder_joined_on_month_end_date_month_min_seq_no,
    holder_joined_on_month_end_date_month_avg_days_s,
    minutes_since_prev_prev2_start_date_week,
    end_date_year_status_sum_days_since_prev_start_d,
    days_since_prev_holder_joined_on_2,
    FLOOR(rn / 2) * 2 AS rn_bucket
FROM windowed_2
WHERE end_date_percent_rank > 0.2
ORDER BY next2_start_date_week DESC, holder_joined_on_month_end_date_month_min_seq_no DESC NULLS FIRST, id
LIMIT 10
