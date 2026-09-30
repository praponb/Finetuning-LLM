WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', status, product) AS status_product_label
    FROM policies
    WHERE premium >= 1042.5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        MAX(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        ROUND(PERCENT_RANK() OVER w_end_date_month_holder_id_by, 3) AS start_date_percent_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY MONTH(start_date), holder_id), 2) AS premium_vs_start_date_month_holder_id_avg,
        LAG(premium) OVER (PARTITION BY MONTH(end_date), holder_id ORDER BY start_date DESC NULLS FIRST, id) AS prev_premium,
        MIN(premium) OVER (PARTITION BY status) AS status_min_premium,
        NTILE(4) OVER (PARTITION BY status, YEAR(start_date) ORDER BY premium DESC, id) AS premium_quartile,
        MIN(premium) OVER w_start_date_month_holder_id AS start_date_month_holder_id_min_premium
    FROM recent
    WINDOW w_end_date_month_holder_id_by AS (PARTITION BY MONTH(end_date), holder_id ORDER BY start_date DESC NULLS FIRST), w_start_date_month_holder_id AS (PARTITION BY MONTH(start_date), holder_id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        end_date,
        max_premium_so_far,
        start_date_percent_rank,
        premium_vs_start_date_month_holder_id_avg,
        prev_premium,
        status_min_premium,
        premium_quartile,
        start_date_month_holder_id_min_premium,
        MAX(prev_premium) OVER (PARTITION BY premium_quartile ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev_premium_max_2,
        ROW_NUMBER() OVER w_premium_quartile_by_premium_2 AS seq_no,
        DENSE_RANK() OVER (PARTITION BY premium_quartile ORDER BY premium DESC) AS premium_dense_rank,
        RANK() OVER w_premium_quartile_by_prev_pre AS prev_premium_rank
    FROM metrics
    WINDOW w_premium_quartile_by_premium_2 AS (PARTITION BY premium_quartile ORDER BY premium DESC, id), w_premium_quartile_by_prev_pre AS (PARTITION BY premium_quartile ORDER BY prev_premium DESC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        premium_dense_rank,
        ROUND(STDDEV(premium_quartile), 2) AS stddev_premium_quartile,
        ROUND(STDDEV(prev_premium), 2) AS stddev_prev_premium
    FROM ranked
    WHERE status_min_premium BETWEEN 15140.0 AND 37550.0
    GROUP BY premium_dense_rank
    HAVING AVG(max_premium_so_far) > 10160.0
)
SELECT premium_dense_rank, stddev_premium_quartile, stddev_prev_premium
FROM rollup_base
WHERE stddev_premium_quartile > 1.0
