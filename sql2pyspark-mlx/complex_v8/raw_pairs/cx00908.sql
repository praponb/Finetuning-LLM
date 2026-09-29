WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status
    FROM policies
    WHERE premium <= 3521.8
      AND start_date >= DATE '2024-10-22'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        RANK() OVER (PARTITION BY status ORDER BY premium ASC, id) AS premium_rank,
        SUM(premium) OVER w_end_date_month AS end_date_month_sum_premium,
        FIRST_VALUE(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        MIN(premium) OVER (PARTITION BY holder_id) AS holder_id_min_premium,
        SUM(premium) OVER w_holder_id AS holder_id_sum_premium,
        LAG(premium, 2) OVER (ORDER BY end_date DESC NULLS FIRST, id) AS prev2_premium,
        LEAD(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date, id) AS next_premium,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date) AS end_date_rank,
        LAG(end_date) OVER (PARTITION BY status ORDER BY premium ASC, id) AS prev_end_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id,
        MAX(premium) OVER w_holder_id AS holder_id_max_premium,
        ROUND(premium - AVG(premium) OVER w_end_date_month, 2) AS premium_vs_end_date_month_avg
    FROM prepared_policys
    WINDOW w_end_date_month AS (PARTITION BY MONTH(end_date)), w_holder_id AS (PARTITION BY holder_id), w_status_by_premium AS (PARTITION BY status ORDER BY premium ASC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        premium_rank,
        end_date_month_sum_premium,
        first_premium,
        holder_id_min_premium,
        holder_id_sum_premium,
        prev2_premium,
        next_premium,
        prev_end_date,
        premium_share_of_holder_id,
        holder_id_max_premium,
        premium_vs_end_date_month_avg
    FROM windowed
    WHERE end_date_rank = 1
),
totals AS (
    -- Summarise per group
    SELECT
        premium_rank,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        COUNT(*) AS n_rows,
        SUM(holder_id_sum_premium) AS total_holder_id_sum_premium,
        PERCENTILE_APPROX(prev2_premium, 0.5) AS median_prev2_premium,
        MAX(end_date) AS last_end_date,
        ROUND(SUM(prev2_premium), 2) AS total_prev2_premium
    FROM top_rows
    WHERE first_premium < 4417.0
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    premium_rank,
    end_date_ym,
    n_rows,
    total_holder_id_sum_premium,
    median_prev2_premium,
    last_end_date,
    total_prev2_premium,
    DATE_FORMAT(last_end_date, 'yyyy-MM-dd') AS last_end_date_label,
    ABS(n_rows - 8) AS n_rows_distance
FROM totals
WHERE total_prev2_premium BETWEEN 3180.0 AND 21060.0
ORDER BY last_end_date_label, n_rows_distance NULLS LAST
