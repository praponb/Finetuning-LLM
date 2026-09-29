WITH valid AS (
    SELECT *, ROUND(revenue * 1.15, 2) AS revenue_scaled
    FROM store_sales
    WHERE product_id = 6
),
cleaned AS (
    SELECT *, revenue_scaled * revenue AS revenue_scaled_times_revenue
    FROM valid
    WHERE store_id IN (1, 2, 5, 6)
),
scored AS (
    SELECT
        id,
        revenue,
        LEAD(revenue_scaled) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_revenue_scaled,
        LAG(revenue_scaled) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_revenue_scaled,
        LEAD(revenue_scaled_times_revenue) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_revenue_scaled_times_revenue,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS event_seq,
        AVG(revenue_scaled) OVER (PARTITION BY MONTH(sale_date) ORDER BY revenue_scaled_times_revenue, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_scaled_avg_5
    FROM cleaned
    WHERE revenue_scaled_times_revenue > 31
),
enriched AS (
    SELECT
        *,
        FIRST_VALUE(prev_revenue_scaled, TRUE) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_revenue_scaled,
        ROW_NUMBER() OVER (ORDER BY next_revenue_scaled, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY revenue_scaled_avg_5 DESC) AS revenue_scaled_avg_5_dense_rank
    FROM scored
),
base AS (
    SELECT *, CEIL(revenue_scaled_avg_5_dense_rank) AS revenue_scaled_avg_5_dense_rank_whole
    FROM enriched
    WHERE prev_revenue_scaled BETWEEN 26 AND 58
)
SELECT
    seq_no,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(next_revenue_scaled, ARRAY(0.25, 0.5, 0.75)) AS next_revenue_scaled_quartiles
FROM base
GROUP BY seq_no
