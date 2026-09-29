WITH with_windows AS (
    SELECT
        *,
        AVG(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) revenue_avg_5_prior,
        DENSE_RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue DESC NULLS FIRST) revenue_dense_rank
    FROM store_sales
),
filtered AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_avg_5_prior,
        revenue_dense_rank,
        COALESCE(revenue_avg_5_prior, 0) revenue_avg_5_prior_filled
    FROM with_windows
    WHERE sale_date >= TO_DATE('2023-06-30')
      AND store_id = 5
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue_avg_5_prior,
        revenue_dense_rank,
        revenue_avg_5_prior_filled,
        AVG(revenue_dense_rank) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) revenue_dense_rank_avg_5_prior,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_revenue,
        AVG(revenue_avg_5_prior) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) revenue_avg_5_prior_avg_6,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY revenue_avg_5_prior DESC NULLS LAST) revenue_avg_5_prior_percent_rank,
        LAG(revenue_avg_5_prior_filled, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) prev_revenue_avg_5_prior_filled
    FROM filtered
    WHERE revenue BETWEEN 2200 AND 2300
),
with_windows_store_sales AS (
    SELECT
        id,
        store_id,
        revenue_avg_5_prior,
        revenue_avg_5_prior_filled,
        revenue_dense_rank_avg_5_prior,
        running_revenue,
        revenue_avg_5_prior_percent_rank,
        prev_revenue_avg_5_prior_filled,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_avg_5_prior DESC, id) event_seq,
        LAST_VALUE(revenue_avg_5_prior_filled, TRUE) OVER (PARTITION BY product_id ORDER BY revenue_dense_rank_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_revenue_avg_5_prior_filled
    FROM metrics
),
cleaned AS (
    SELECT
        id,
        store_id,
        revenue_dense_rank_avg_5_prior,
        running_revenue,
        revenue_avg_5_prior_percent_rank,
        event_seq,
        last_known_revenue_avg_5_prior_filled,
        running_revenue + event_seq running_revenue_plus_event_seq
    FROM with_windows_store_sales
    WHERE revenue_dense_rank_avg_5_prior <> 4
),
scored AS (
    SELECT
        id,
        store_id,
        revenue_dense_rank_avg_5_prior,
        AVG(running_revenue) OVER (PARTITION BY store_id) store_id_avg_running_revenue,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY revenue_avg_5_prior_percent_rank, id) revenue_avg_5_prior_percent_rank_bucket,
        LEAD(revenue_avg_5_prior_percent_rank, 1) OVER (ORDER BY revenue_avg_5_prior_percent_rank, id) next_revenue_avg_5_prior_percent_rank
    FROM cleaned
),
joined AS (
    SELECT
        e.*,
        p.sqft,
        p.region,
        p.store_name
    FROM scored e
    JOIN stores p
        ON e.store_id = p.id
),
grouped AS (
    SELECT
        CASE WHEN revenue_dense_rank_avg_5_prior >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_dense_rank_avg_5_prior_tier,
        APPROX_PERCENTILE(store_id_avg_running_revenue, ARRAY(0.25, 0.5, 0.75)) AS store_id_avg_running_revenue_quartiles,
        SUM(CASE WHEN region = 'East' THEN revenue_dense_rank_avg_5_prior ELSE 0 END) AS east_revenue_dense_rank_avg_5_prior,
        ROUND(AVG(sqft), 2) AS avg_sqft
    FROM joined
    GROUP BY 1
    HAVING ROUND(AVG(sqft), 2) >= 0
)
SELECT revenue_dense_rank_avg_5_prior_tier, store_id_avg_running_revenue_quartiles
FROM grouped
ORDER BY revenue_dense_rank_avg_5_prior_tier DESC NULLS FIRST
LIMIT 20
