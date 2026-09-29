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
        CASE WHEN product IN ('HOME', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        ABS(premium - 3465.4) AS premium_distance
    FROM policies
    WHERE start_date IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY YEAR(end_date) ORDER BY end_date DESC NULLS FIRST, id) AS end_date_tercile,
        RANK() OVER (PARTITION BY MONTH(end_date), product_group ORDER BY premium, id) AS premium_rank,
        ROUND(AVG(premium_distance) OVER (PARTITION BY YEAR(start_date), product_group ORDER BY end_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_distance_avg_3,
        LAG(premium_distance, 3) OVER (ORDER BY end_date DESC, id) AS prev3_premium_distance,
        ROW_NUMBER() OVER (PARTITION BY YEAR(start_date), product_group ORDER BY end_date DESC, id) AS rn,
        SIZE(COLLECT_SET(product_group) OVER (PARTITION BY product)) AS distinct_product_group_in_product,
        ROW_NUMBER() OVER (ORDER BY end_date DESC, id) AS end_date_seq,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product), 2) AS premium_vs_product_avg,
        premium_distance - LAG(premium_distance) OVER (PARTITION BY MONTH(end_date), product_group ORDER BY premium, id) AS premium_distance_change,
        RANK() OVER w_end_date_year_by_end_date_ti AS end_date_rank,
        MAX(premium_distance) OVER (PARTITION BY MONTH(end_date), product_group ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_distance_max_2_prior,
        ROUND(premium_distance - AVG(premium_distance) OVER (PARTITION BY holder_id), 2) AS premium_distance_vs_holder_id_avg,
        NTH_VALUE(premium, 2) OVER (PARTITION BY YEAR(start_date), product_group ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY YEAR(start_date), product_group ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM scoped
    WINDOW w_end_date_month_product_group AS (PARTITION BY MONTH(end_date), product_group ORDER BY premium, id), w_all_by_end_date AS (ORDER BY end_date DESC, id), w_end_date_year_by_end_date_ti AS (PARTITION BY YEAR(end_date) ORDER BY end_date DESC NULLS FIRST)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE end_date_seq = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, rn)) AS distinct_holder_id_in_product_rn,
        SUM(end_date_tercile) OVER (PARTITION BY product, rn) AS product_rn_sum_end_date_tercile,
        LAST_VALUE(rn) OVER (PARTITION BY end_date_seq ORDER BY prev3_premium_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rn,
        MIN(premium_distance_max_2_prior) OVER (PARTITION BY product, rn) AS product_rn_min_premium_distance_max_2_prior,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_rn, 0), 4) AS premium_share_of_product_rn,
        ROUND(PERCENT_RANK() OVER w_end_date_rank_by_start_date, 3) AS start_date_percent_rank
    FROM first_rows
    WINDOW w_product_rn AS (PARTITION BY product, rn), w_end_date_rank_by_start_date AS (PARTITION BY end_date_rank ORDER BY start_date DESC, id)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    product_group,
    premium_distance,
    end_date_tercile,
    premium_rank,
    prev3_premium_distance,
    rn,
    premium_vs_product_avg,
    second_premium,
    distinct_holder_id_in_product_rn,
    product_rn_sum_end_date_tercile,
    last_rn,
    start_date_percent_rank,
    COALESCE(product_group, 'UNKNOWN') AS product_group_or_unknown
FROM metrics
ORDER BY holder_id DESC, end_date_tercile
