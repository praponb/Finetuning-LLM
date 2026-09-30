WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE end_date < DATE '2026-10-23'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(end_date) OVER (PARTITION BY product, status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        MAX(premium) OVER w_holder_id_status AS holder_id_status_max_premium,
        MAX(premium) OVER (PARTITION BY status, MONTH(start_date) ORDER BY start_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_max_3,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date), 3) AS start_date_percent_rank,
        RANK() OVER w_all_by_start_date AS start_date_rank,
        ROUND(AVG(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        FIRST_VALUE(start_date) OVER (PARTITION BY status, holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, YEAR(end_date)), 0), 4) AS premium_share_of_holder_id_end_date_year,
        SUM(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        MIN(premium) OVER (PARTITION BY status, holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_start_date_month_by_s_2) AS days_since_prev_start_date,
        LAST_VALUE(start_date) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date) ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        LAG(start_date, 2) OVER (PARTITION BY holder_id, status ORDER BY premium NULLS LAST, id) AS prev2_start_date
    FROM recent
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status), w_all_by_start_date AS (ORDER BY start_date, id), w_status_start_date_month_by_s_2 AS (PARTITION BY status, MONTH(start_date) ORDER BY start_date DESC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_clean,
        last_end_date,
        holder_id_status_max_premium,
        premium_max_3,
        start_date_percent_rank,
        premium_avg_3_prior,
        premium_vs_status_avg,
        first_start_date,
        premium_share_of_holder_id_end_date_year,
        premium_sum_3_prior,
        min_premium_so_far,
        days_since_prev_start_date,
        last_start_date,
        prev2_start_date
    FROM with_windows
    WHERE start_date_rank <= 2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(start_date_percent_rank) OVER (PARTITION BY YEAR(prev2_start_date) ORDER BY last_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_start_date_percent_rank_so_far,
        RANK() OVER w_all_by_premium_sum_3_prior_t AS premium_sum_3_prior_rank,
        DENSE_RANK() OVER (ORDER BY start_date_percent_rank ASC) AS start_date_percent_rank_dense_rank,
        ROW_NUMBER() OVER (ORDER BY premium_sum_3_prior DESC, id) AS seq_no
    FROM top_rows
    WINDOW w_all_by_premium_sum_3_prior_t AS (ORDER BY premium_sum_3_prior DESC)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    status,
    last_end_date,
    holder_id_status_max_premium,
    premium_max_3,
    start_date_percent_rank,
    premium_avg_3_prior,
    first_start_date,
    premium_sum_3_prior,
    min_premium_so_far,
    days_since_prev_start_date,
    last_start_date,
    max_start_date_percent_rank_so_far,
    premium_sum_3_prior_rank,
    COALESCE(premium, 0.0) AS premium_filled
FROM ranked
WHERE days_since_prev_start_date >= 80
ORDER BY holder_id DESC
