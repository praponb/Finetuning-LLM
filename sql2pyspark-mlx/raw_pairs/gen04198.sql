WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        c.created_at,
        li_item.product_id,
        li_item.qty,
        li_item.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) t AS li_item
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(qty) OVER (PARTITION BY DATE_TRUNC('WEEK', created_at) ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_count_1h,
        COUNT(qty) OVER (PARTITION BY customer_id) AS customer_id_count_qty
    FROM exploded
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) < '2023-05-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        product_id,
        customer_id_count_qty,
        PERCENT_RANK() OVER w_ordered AS product_id_percent_rank,
        MAX(qty) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_max_2h
    FROM verified
    WHERE promo NOT IN ('VIP', 'SPRING10', 'BUNDLE')
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY product_id DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        product_id,
        product_id_percent_rank,
        ROUND(product_id, 0) AS product_id_rounded,
        ROUND(customer_id_count_qty * 100, 2) AS customer_id_count_qty_scaled
    FROM with_windows_carts
    WHERE TO_DATE(created_at) < '2024-10-05'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, customer_id_count_qty_scaled + product_id AS customer_id_count_qty_scaled_plus_product_id
    FROM cleaned
    WHERE created_at >= DATE '2025-03-02'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        created_at,
        product_id,
        DENSE_RANK() OVER (ORDER BY product_id ASC NULLS FIRST) AS product_id_dense_rank,
        COUNT(customer_id_count_qty_scaled) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS customer_id_count_qty_scaled_count_2h,
        SUM(product_id_percent_rank) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_percent_rank_sum_15m,
        CUME_DIST() OVER (ORDER BY product_id ASC NULLS LAST) AS product_id_cume_dist,
        AVG(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_avg_1h
    FROM valid
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN product_id_cume_dist >= 0.67 THEN 'LARGE' ELSE 'SMALL' END AS product_id_cume_dist_tier,
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        MAX(created_at) AS last_created_at,
        MAX(created_at) AS last_seen,
        ROUND(AVG(product_id), 2) AS avg_product_id,
        APPROX_PERCENTILE(product_id_dense_rank, 0.5) AS product_id_dense_rank_p50,
        PERCENTILE_APPROX(customer_id_count_qty_scaled_count_2h, ARRAY(0.25, 0.5, 0.75)) AS customer_id_count_qty_scaled_count_2h_quartiles
    FROM metrics
    GROUP BY CASE WHEN product_id_cume_dist >= 0.67 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('HOUR', created_at)
)
SELECT
    product_id_cume_dist_tier,
    hour_window,
    last_created_at,
    last_seen,
    product_id_dense_rank_p50
FROM stats
ORDER BY hour_window DESC;
