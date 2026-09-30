WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 3544.1 THEN 'large' WHEN premium >= 2096.6 THEN 'medium' ELSE 'small' END AS premium_tier,
        CASE WHEN premium >= 3532.7 THEN 'HIGH' WHEN premium >= 2111.3 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier_2
    FROM policies
    WHERE premium > 1042.8
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        end_date,
        premium_tier_2,
        LAG(start_date) OVER w_all_by_end_date AS prev_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY status ORDER BY premium NULLS LAST, id)) AS days_since_prev_start_date,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        NTH_VALUE(end_date, 2) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        SUM(premium) OVER (PARTITION BY premium_tier ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        RANK() OVER (PARTITION BY premium_tier_2, status ORDER BY premium DESC) AS premium_rank,
        NTILE(3) OVER (PARTITION BY premium_tier ORDER BY end_date, id) AS end_date_tercile,
        ROUND(premium / NULLIF(SUM(premium) OVER w_status, 0), 4) AS premium_share_of_status,
        LEAD(premium, 2) OVER (PARTITION BY premium_tier_2, status ORDER BY premium DESC, id) AS next2_premium,
        SUM(premium) OVER (PARTITION BY status) AS status_sum_premium,
        DENSE_RANK() OVER (PARTITION BY premium_tier_2, status ORDER BY premium DESC, id) AS premium_dense_rank,
        ROUND(AVG(premium) OVER (ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY premium NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior
    FROM prepared
    WINDOW w_all_by_end_date AS (ORDER BY end_date, id), w_status AS (PARTITION BY status)
),
stats AS (
    -- Summarise per group
    SELECT
        premium_rank,
        DATEDIFF(MAX(second_end_date), MIN(second_end_date)) AS second_end_date_span_days,
        COUNT(*) AS cnt,
        ROUND(SUM(next2_premium), 2) AS total_next2_premium,
        PERCENTILE_APPROX(status_sum_premium, 0.5) AS median_status_sum_premium
    FROM windowed
    WHERE holder_id <= 11
    GROUP BY 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(second_end_date_span_days) OVER (PARTITION BY premium_rank, cnt ORDER BY second_end_date_span_days, premium_rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_end_date_span_days,
        SUM(second_end_date_span_days) OVER (PARTITION BY premium_rank, cnt ORDER BY second_end_date_span_days, premium_rank ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS second_end_date_span_days_sum_3_prior,
        ROUND(AVG(total_next2_premium) OVER w_cnt_premium_rank, 2) AS cnt_premium_rank_avg_total_next2_premium,
        ROUND(second_end_date_span_days / NULLIF(SUM(second_end_date_span_days) OVER (PARTITION BY cnt, premium_rank), 0), 4) AS second_end_date_span_days_share_of_cnt_premium_r
    FROM stats
    WINDOW w_cnt_premium_rank AS (PARTITION BY cnt, premium_rank)
)
SELECT
    premium_rank,
    second_end_date_span_days,
    cnt,
    median_status_sum_premium,
    second_end_date_span_days_sum_3_prior,
    second_end_date_span_days_share_of_cnt_premium_r,
    ROUND(total_next2_premium * 1.07, 2) AS total_next2_premium_adj,
    COALESCE(total_next2_premium, 0.0) AS total_next2_premium_filled
FROM with_trends
