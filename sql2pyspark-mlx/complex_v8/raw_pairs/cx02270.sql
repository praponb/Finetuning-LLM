WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_holder_id_product_by_start_d AS start_date_rank,
        SUM(premium) OVER (PARTITION BY status, holder_id ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_sum_2_prior,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY product, holder_id ORDER BY premium DESC, id)) AS days_since_prev_start_date,
        SUM(premium) OVER (PARTITION BY product, holder_id ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_sum_2,
        ROUND(CUME_DIST() OVER (PARTITION BY product, holder_id ORDER BY premium DESC), 3) AS premium_cume_dist,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        NTILE(10) OVER w_status_holder_id_by_premium_2 AS premium_decile,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM scoped
    WINDOW w_holder_id_product_by_start_d AS (PARTITION BY holder_id, product ORDER BY start_date ASC), w_status_holder_id_by_premium_2 AS (PARTITION BY status, holder_id ORDER BY premium, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        start_date_rank,
        premium_sum_2_prior,
        first_start_date,
        premium_vs_holder_id_avg,
        days_since_prev_start_date,
        premium_cume_dist,
        rows_in_holder_id,
        second_start_date,
        CONCAT(status, '#', CAST(start_date_rank AS STRING)) AS status_ref,
        CASE WHEN status IN ('LAPSED', 'EXPIRED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        QUARTER(second_start_date) AS second_start_date_quarter
    FROM metrics
    WHERE start_date IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        start_date_rank,
        premium_sum_2_prior,
        premium_cume_dist,
        rows_in_holder_id,
        second_start_date,
        status_ref,
        status_group,
        second_start_date_quarter,
        RANK() OVER w_start_date_rank_holder_id_by AS second_start_date_rank,
        ROUND(CUME_DIST() OVER w_all_by_premium_sum_2_prior, 3) AS premium_sum_2_prior_cume_dist,
        ROUND(CUME_DIST() OVER w_start_date_rank_holder_id_by, 3) AS second_start_date_cume_dist,
        COUNT(*) OVER (PARTITION BY status_group) AS rows_in_status_group,
        ROUND(AVG(premium) OVER (PARTITION BY start_date_rank, holder_id ORDER BY second_start_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(first_start_date) ORDER BY premium_sum_2_prior), 3) AS premium_sum_2_prior_cume_dist_2
    FROM scored
    WINDOW w_start_date_rank_holder_id_by AS (PARTITION BY start_date_rank, holder_id ORDER BY second_start_date ASC), w_all_by_premium_sum_2_prior AS (ORDER BY premium_sum_2_prior DESC NULLS FIRST, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        start_date_rank,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        second_start_date_rank,
        ROUND(AVG(premium), 2) AS avg_premium,
        MAX(premium_sum_2_prior_cume_dist) AS max_premium_sum_2_prior_cume_dist,
        ROUND(SUM(premium_sum_2_prior_cume_dist) / NULLIF(SUM(second_start_date_cume_dist), 0), 3) AS premium_sum_2_prior_cume_dist_to_second_start_da
    FROM ranked
    WHERE status_ref IS NOT NULL
    GROUP BY 1, 2, 3
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        start_date_rank,
        start_date_month,
        second_start_date_rank,
        ROW_NUMBER() OVER (ORDER BY max_premium_sum_2_prior_cume_dist DESC NULLS FIRST, start_date_rank, start_date_month, second_start_date_rank) AS rn,
        RANK() OVER (ORDER BY max_premium_sum_2_prior_cume_dist DESC NULLS FIRST) AS max_premium_sum_2_prior_cume_dist_rank,
        LEAD(start_date_rank, 3) OVER (ORDER BY max_premium_sum_2_prior_cume_dist DESC NULLS FIRST, start_date_rank, start_date_month, second_start_date_rank) AS next3_start_date_rank
    FROM per_group
)
-- Final projection
SELECT
    start_date_rank,
    rn,
    max_premium_sum_2_prior_cume_dist_rank,
    next3_start_date_rank,
    ABS(second_start_date_rank - 5) AS second_start_date_rank_distance
FROM metrics_2
ORDER BY start_date_rank
