WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER w_running AS running_avg_cost,
        MAX(cost) OVER w_running AS running_max_cost,
        SUM(cost) OVER w_running AS running_sum_cost,
        COUNT(cost) OVER w_running AS running_count_cost,
        CORR(stock_qty, cost) OVER w_running_2 AS corr_stock_qty_cost
    FROM products
    WHERE supplier_id IS NOT NULL
    WINDOW w_running AS (PARTITION BY supplier_id, brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY DATE_TRUNC('month', launch_date), category)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock <= s.launch_date
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
        )
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.unit_price
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Running and moving aggregates
SELECT
    id,
    product_name,
    category,
    brand,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    running_sum_cost,
    running_count_cost,
    quantity,
    unit_price,
    LEAD(price, 2) OVER w_hist AS next_price,
    PERCENT_RANK() OVER w_grp AS stock_qty_percent_rank,
    COUNT(running_avg_cost) OVER (PARTITION BY category) AS category_count_running_avg_cost
FROM latest_match
WINDOW w_hist AS (PARTITION BY category ORDER BY unit_price, id), w_grp AS (PARTITION BY YEAR(launch_date), supplier_id ORDER BY stock_qty DESC NULLS FIRST);
