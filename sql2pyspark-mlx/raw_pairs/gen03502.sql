WITH scored AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        MAX(revenue) OVER w_recent AS last3_max_revenue,
        COUNT(revenue) OVER w_recent AS last3_count_revenue,
        SUM(revenue) OVER w_recent AS last3_sum_revenue,
        MIN(revenue) OVER w_recent AS last3_min_revenue,
        ROW_NUMBER() OVER (PARTITION BY YEAR(sale_date), store_id ORDER BY units, id) AS event_seq
    FROM store_sales
    WINDOW w_recent AS (PARTITION BY product_id, store_id ORDER BY revenue, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last3_count_revenue DESC, id) = 1
),
valid AS (
    SELECT
        *,
        GREATEST(event_seq, units) AS greatest_event_seq_units,
        ROUND(last3_max_revenue * 0.85, 2) AS last3_max_revenue_scaled
    FROM deduped
    WHERE sale_date >= DATE '2023-08-27'
),
scored_store_sales AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last3_sum_revenue DESC NULLS FIRST) AS last3_sum_revenue_dense_rank,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY units DESC, id) AS units_bucket,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        LAG(last3_sum_revenue, 2) OVER w_running AS prev_last3_sum_revenue
    FROM valid
    WHERE MONTH(sale_date) IN (4, 8, 9)
    WINDOW w_running AS (PARTITION BY product_id ORDER BY last3_max_revenue_scaled, id)
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY greatest_event_seq_units DESC, id) AS seq_no,
        ROUND(last3_sum_revenue_dense_rank / SUM(last3_sum_revenue_dense_rank) OVER (PARTITION BY MONTH(sale_date), product_id), 4) AS last3_sum_revenue_dense_rank_share
    FROM scored_store_sales
)
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    APPROX_PERCENTILE(last3_count_revenue, ARRAY(0.25, 0.5, 0.75)) AS last3_count_revenue_quartiles,
    COUNT(*) AS event_count,
    MAX(running_revenue) AS max_running_revenue
FROM enriched
GROUP BY 1;
