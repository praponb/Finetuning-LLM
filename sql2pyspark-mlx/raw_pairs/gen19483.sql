WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        units,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
        MIN(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS row_seq
    FROM store_sales
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        row_seq,
        AVG(running_min_revenue) OVER (ORDER BY running_sum_revenue, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_min_revenue_avg_3_prior,
        LAG(running_min_revenue, 2) OVER (ORDER BY running_sum_revenue, id) AS prev_running_min_revenue,
        FIRST_VALUE(running_min_revenue) IGNORE NULLS OVER w_recent AS first_known_running_min_revenue
    FROM enriched
    WHERE units BETWEEN 15 AND 20
    WINDOW w_recent AS (ORDER BY running_sum_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        prev_running_min_revenue,
        first_known_running_min_revenue,
        SUM(prev_running_min_revenue) OVER (ORDER BY running_min_revenue_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_running_min_revenue,
        MAX(prev_running_min_revenue) OVER (ORDER BY running_min_revenue_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_running_min_revenue,
        COUNT(prev_running_min_revenue) OVER (ORDER BY running_min_revenue_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_running_min_revenue
    FROM enriched_store_sales
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        prev_running_min_revenue,
        first_known_running_min_revenue,
        running_max_prev_running_min_revenue,
        MIN(running_count_prev_running_min_revenue) OVER w_recent AS last7_min_running_count_prev_running_min_revenue,
        SUM(running_count_prev_running_min_revenue) OVER w_recent AS last7_sum_running_count_prev_running_min_revenue,
        AVG(running_count_prev_running_min_revenue) OVER w_recent AS last7_avg_running_count_prev_running_min_revenue,
        MAX(running_count_prev_running_min_revenue) OVER w_recent AS last7_max_running_count_prev_running_min_revenue
    FROM scored
    WINDOW w_recent AS (ORDER BY running_count_prev_running_min_revenue, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    prev_running_min_revenue,
    last7_sum_running_count_prev_running_min_revenue,
    last7_avg_running_count_prev_running_min_revenue,
    last7_max_running_count_prev_running_min_revenue
FROM with_windows
ORDER BY last7_avg_running_count_prev_running_min_revenue DESC NULLS FIRST, prev_running_min_revenue DESC;
