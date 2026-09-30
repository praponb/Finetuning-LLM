WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(stock_qty) OVER w_ordered AS running_sum_stock_qty,
        COUNT(stock_qty) OVER w_ordered AS running_count_stock_qty,
        AVG(stock_qty) OVER w_ordered AS running_avg_stock_qty
    FROM products
    WHERE category = 'Books'
    WINDOW w_ordered AS (PARTITION BY supplier_id, category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        running_sum_stock_qty,
        running_count_stock_qty,
        DATE_TRUNC('YEAR', launch_date) AS launch_date_period
    FROM scored
    WHERE launch_date > TO_DATE('2023-03-27')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date < s.launch_date_period
        )
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(launch_date_period) OVER (PARTITION BY category ORDER BY launch_date_period, id) IS NULL
                 OR launch_date_period > LAG(launch_date_period) OVER (PARTITION BY category ORDER BY launch_date_period, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM not_flagged
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        product_name,
        category,
        brand,
        launch_date,
        tags,
        running_sum_stock_qty,
        running_count_stock_qty,
        launch_date_period,
        SUM(session_start_flag) OVER (PARTITION BY category ORDER BY launch_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.units,
        latest.revenue
    FROM sessionized s
    JOIN LATERAL (
            SELECT c.units, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.inventory_quantity
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        tags,
        running_count_stock_qty,
        launch_date_period,
        island_id,
        units,
        revenue,
        inventory_quantity,
        ROW_NUMBER() OVER w_part AS row_seq,
        AVG(running_count_stock_qty) OVER (PARTITION BY brand) AS brand_avg_running_count_stock_qty,
        DENSE_RANK() OVER (ORDER BY running_sum_stock_qty NULLS LAST) AS running_sum_stock_qty_dense_rank
    FROM with_latest
    WINDOW w_part AS (PARTITION BY brand ORDER BY running_count_stock_qty, id)
)
SELECT
    id,
    running_count_stock_qty,
    launch_date_period,
    island_id,
    revenue,
    inventory_quantity,
    row_seq
FROM with_windows;
