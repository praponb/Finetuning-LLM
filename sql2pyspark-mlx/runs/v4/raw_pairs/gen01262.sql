WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS seq_no,
        NTILE(4) OVER w_part AS units_bucket,
        LAG(revenue, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue,
        PERCENT_RANK() OVER w_running AS units_percent_rank
    FROM store_sales
    WINDOW w_part AS (PARTITION BY store_id ORDER BY units DESC, id), w_running AS (PARTITION BY product_id ORDER BY units DESC NULLS FIRST)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(units_percent_rank) OVER (PARTITION BY product_id ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS units_percent_rank_avg_5,
        LAST_VALUE(revenue, TRUE) OVER (PARTITION BY product_id ORDER BY prev_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY units_percent_rank DESC, id) AS units_percent_rank_bucket
    FROM scored
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY revenue NULLS FIRST) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY prev_revenue DESC, id) AS prev_revenue_bucket,
        LAG(units_bucket, 1) OVER (PARTITION BY product_id ORDER BY rank_in_group, id) AS prev_units_bucket,
        COUNT(units_percent_rank_bucket) OVER (PARTITION BY store_id, product_id) AS store_id_count_units_percent_rank_bucket,
        SUM(prev_revenue) OVER (PARTITION BY product_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_revenue
    FROM latest
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(units_percent_rank_avg_5, 1) OVER (ORDER BY last_known_revenue, id) AS prev_units_percent_rank_avg_5,
        ROUND(store_id_count_units_percent_rank_bucket / SUM(store_id_count_units_percent_rank_bucket) OVER (PARTITION BY product_id, store_id), 4) AS store_id_count_units_percent_rank_bucket_share
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    SUM(units) AS total_units,
    ROUND(AVG(store_id_count_units_percent_rank_bucket), 2) AS avg_store_id_count_units_percent_rank_bucket,
    COUNT(*) AS cnt
FROM enriched
GROUP BY activity_month
ORDER BY cnt NULLS FIRST;
