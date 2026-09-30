WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product, holder_id)) AS distinct_status_in_product_holder_id,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        SUM(premium) OVER (PARTITION BY status ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_sum_6_prior,
        LAG(premium, 2) OVER (PARTITION BY holder_id, status ORDER BY premium ASC, id) AS prev2_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(start_date), product), 0), 4) AS premium_share_of_start_date_year_product,
        ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY premium ASC, id) AS seq_no,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id
    FROM recent
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE seq_no = 1
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        start_date,
        status,
        distinct_status_in_product_holder_id,
        running_premium,
        first_known_premium,
        prev2_premium,
        premium_share_of_start_date_year_product,
        seq_no,
        premium_share_of_holder_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(start_date) ORDER BY running_premium ASC), 3) AS running_premium_percent_rank,
        NTILE(4) OVER w_holder_id_distinct_status_in AS premium_share_of_holder_id_quartile,
        RANK() OVER (PARTITION BY MONTH(start_date) ORDER BY running_premium ASC, id) AS running_premium_rank
    FROM top_rows
    WINDOW w_holder_id_distinct_status_in AS (PARTITION BY holder_id, distinct_status_in_product_holder_id ORDER BY premium_share_of_holder_id ASC, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        running_premium_rank,
        seq_no,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        ROUND(AVG(running_premium_rank), 2) AS avg_running_premium_rank,
        SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS n_status_cancelled,
        MIN(distinct_status_in_product_holder_id) AS min_distinct_status_in_product_holder_id,
        ROUND(SUM(first_known_premium), 2) AS total_first_known_premium,
        COUNT(*) AS cnt
    FROM with_windows_2
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    running_premium_rank,
    seq_no,
    start_date_month,
    min_distinct_status_in_product_holder_id,
    total_first_known_premium,
    cnt,
    ROUND(cnt / NULLIF(avg_running_premium_rank, 0), 3) AS cnt_per_avg_running_premium_rank,
    ROUND(cnt / NULLIF(running_premium_rank, 0), 3) AS cnt_per_running_premium_rank
FROM rollup_base
WHERE HOUR(start_date_month) BETWEEN 10 AND 12
ORDER BY total_first_known_premium DESC
