WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT a.*, s.city
    FROM products a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_qty_avg_2,
        RANK() OVER w_running AS quantity_rank,
        AVG(stock_qty) OVER (PARTITION BY MONTH(launch_date), supplier_id ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_6_prior
    FROM with_last
    WINDOW w_running AS (ORDER BY quantity NULLS FIRST)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity_rank) OVER (PARTITION BY category ORDER BY quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity_rank,
        MAX(quantity_rank) OVER (PARTITION BY category ORDER BY quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity_rank,
        COUNT(quantity_rank) OVER (PARTITION BY category ORDER BY quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity_rank,
        SUM(quantity_rank) OVER (PARTITION BY category ORDER BY quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity_rank,
        MIN(cost) OVER (PARTITION BY supplier_id, category) AS supplier_id_min_cost
    FROM metrics
    WHERE quantity > 4
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN brand = 'Initech' THEN stock_qty_avg_6_prior ELSE 0 END) AS initech_stock_qty_avg_6_prior,
    ROUND(SUM(running_avg_quantity_rank) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS running_avg_quantity_rank_per_globex,
    COUNT_IF(city = 'London') AS london_events
FROM enriched
GROUP BY activity_month
ORDER BY num_rows DESC
