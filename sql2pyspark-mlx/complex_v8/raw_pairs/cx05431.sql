WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        CONCAT_WS(': ', product, status) AS product_status_label
    FROM policies
    WHERE (holder_id >= 8 OR status IS NOT NULL)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        days_end_date_to_start_date,
        RANK() OVER (ORDER BY end_date DESC) AS end_date_rank,
        ROUND(AVG(days_end_date_to_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_end_date_to_start_date_avg_4,
        LEAD(premium) OVER (ORDER BY premium DESC NULLS FIRST, id) AS next_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_end_date_month, 0), 4) AS premium_share_of_end_date_month,
        ROW_NUMBER() OVER (PARTITION BY MONTH(end_date), holder_id ORDER BY premium DESC, id) AS premium_seq,
        COUNT(*) OVER w_product AS rows_in_product,
        ROUND(CUME_DIST() OVER (ORDER BY end_date DESC), 3) AS end_date_cume_dist,
        MAX(days_end_date_to_start_date) OVER (ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_end_date_to_start_date_max_2_prior,
        RANK() OVER (ORDER BY premium DESC NULLS FIRST) AS premium_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC NULLS FIRST), 3) AS premium_percent_rank,
        LAG(end_date, 3) OVER w_all_by_end_date AS prev3_end_date,
        ROUND(days_end_date_to_start_date - AVG(days_end_date_to_start_date) OVER w_product, 2) AS days_end_date_to_start_date_vs_product_avg,
        SUM(premium) OVER (PARTITION BY product) AS product_sum_premium,
        MAX(premium) OVER (ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_max_1_prior,
        days_end_date_to_start_date - LAG(days_end_date_to_start_date) OVER w_end_date_month_by_premium AS days_end_date_to_start_date_change
    FROM recent
    WINDOW w_end_date_month AS (PARTITION BY MONTH(end_date)), w_product AS (PARTITION BY product), w_all_by_end_date AS (ORDER BY end_date DESC, id), w_end_date_month_by_premium AS (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY premium, id)
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        days_end_date_to_start_date,
        end_date_rank,
        next_premium,
        premium_share_of_end_date_month,
        premium_seq,
        end_date_cume_dist,
        days_end_date_to_start_date_max_2_prior,
        premium_rank,
        days_end_date_to_start_date_vs_product_avg,
        premium_max_1_prior,
        days_end_date_to_start_date_change,
        LEAD(end_date_rank, 2, 0) OVER (ORDER BY premium_max_1_prior, id) AS next2_end_date_rank,
        LEAD(end_date_rank, 3, 0) OVER w_premium_rank_by_days_end_dat AS next3_end_date_rank,
        COUNT(*) OVER w_premium_seq_end_date_rank AS rows_in_premium_seq_end_date_rank,
        MAX(premium_seq) OVER (ORDER BY end_date_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_seq_so_far
    FROM metrics
    WINDOW w_premium_rank_by_days_end_dat AS (PARTITION BY premium_rank ORDER BY days_end_date_to_start_date_max_2_prior ASC, id), w_premium_seq_end_date_rank AS (PARTITION BY premium_seq, end_date_rank)
)
SELECT
    id,
    holder_id,
    days_end_date_to_start_date,
    end_date_rank,
    next_premium,
    premium_share_of_end_date_month,
    premium_seq,
    end_date_cume_dist,
    days_end_date_to_start_date_max_2_prior,
    premium_rank,
    days_end_date_to_start_date_vs_product_avg,
    premium_max_1_prior,
    next2_end_date_rank,
    next3_end_date_rank,
    rows_in_premium_seq_end_date_rank,
    max_premium_seq_so_far,
    premium_rank - max_premium_seq_so_far AS premium_rank_minus_max_premium_seq_so_far
FROM metrics_2
WHERE max_premium_seq_so_far > 26
ORDER BY end_date_rank DESC
