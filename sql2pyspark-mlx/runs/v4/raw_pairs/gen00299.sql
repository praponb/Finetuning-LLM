WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
        CEIL(revenue) AS revenue_whole
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-06-21')
      AND sale_date BETWEEN '2024-01-10' AND '2024-04-19'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY revenue_whole) AS revenue_whole_cume_dist,
        RANK() OVER (PARTITION BY product_id, store_id ORDER BY days_since_sale_date NULLS FIRST) AS days_since_sale_date_rank,
        LAG(revenue_whole, 1) OVER (PARTITION BY store_id ORDER BY revenue_whole, id) AS prev_revenue_whole,
        FIRST_VALUE(days_since_sale_date) IGNORE NULLS OVER (ORDER BY revenue_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_sale_date
    FROM base
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, p2.city
    FROM metrics s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(days_since_sale_date_rank / units, 2) AS days_since_sale_date_rank_per_units,
        LEAST(first_known_days_since_sale_date, revenue_whole_cume_dist) AS least_first_known_days_since_sale_date_revenue_whole_cume_dist
    FROM joined
    WHERE sale_date > TO_DATE('2023-06-12')
      AND city IS NOT NULL
),
metrics_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        days_since_sale_date,
        revenue_whole,
        revenue_whole_cume_dist,
        days_since_sale_date_rank,
        prev_revenue_whole,
        first_known_days_since_sale_date,
        city,
        days_since_sale_date_rank_per_units,
        least_first_known_days_since_sale_date_revenue_whole_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS seq_no,
        LAST_VALUE(units, TRUE) OVER (ORDER BY days_since_sale_date_rank_per_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY days_since_sale_date DESC, id) AS row_seq
    FROM cleaned
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        days_since_sale_date,
        revenue_whole,
        revenue_whole_cume_dist,
        days_since_sale_date_rank,
        prev_revenue_whole,
        first_known_days_since_sale_date,
        city,
        days_since_sale_date_rank_per_units,
        least_first_known_days_since_sale_date_revenue_whole_cume_dist,
        seq_no,
        last_known_units,
        row_seq,
        LAG(days_since_sale_date, 2) OVER w_recent AS prev_days_since_sale_date,
        AVG(first_known_days_since_sale_date) OVER (PARTITION BY product_id, store_id ORDER BY days_since_sale_date_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS first_known_days_since_sale_date_avg_6_prior,
        LAG(units, 2) OVER (ORDER BY least_first_known_days_since_sale_date_revenue_whole_cume_dist, id) AS prev_units
    FROM metrics_store_sales
    WINDOW w_recent AS (PARTITION BY city ORDER BY first_known_days_since_sale_date, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', sale_date) AS week_start,
        store_id,
        product_id,
        APPROX_PERCENTILE(least_first_known_days_since_sale_date_revenue_whole_cume_dist, 0.9) AS least_first_known_days_since_sale_date_revenue_whole_cume_dist_p90,
        APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
        GROUPING_ID() AS gid
    FROM scored
    GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', sale_date), store_id, product_id), (DATE_TRUNC('WEEK', sale_date), store_id), (DATE_TRUNC('WEEK', sale_date)), ())
)
SELECT
    store_id,
    least_first_known_days_since_sale_date_revenue_whole_cume_dist_p90,
    revenue_quartiles
FROM stats
ORDER BY least_first_known_days_since_sale_date_revenue_whole_cume_dist_p90 NULLS LAST, store_id DESC;
