WITH joined AS (
    -- Attach products attributes
    SELECT s.*, p.stock_qty
    FROM order_items s
    INNER JOIN products p
        ON s.product_id = p.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        stock_qty,
        COUNT(stock_qty) OVER w_ordered AS running_count_stock_qty,
        SUM(stock_qty) OVER w_ordered AS running_sum_stock_qty,
        AVG(stock_qty) OVER w_ordered AS running_avg_stock_qty,
        MAX(stock_qty) OVER w_ordered AS running_max_stock_qty
    FROM joined
    WHERE product_id = 6
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_max_stock_qty ASC NULLS FIRST, id ASC) = 1
),
joined_order_items AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.quantity,
        a.stock_qty,
        a.running_count_stock_qty,
        a.running_avg_stock_qty,
        d.cost,
        d.stock_qty AS product_stock_qty
    FROM ranked a
    LEFT JOIN products d
        ON a.product_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(running_count_stock_qty, 2) AS running_count_stock_qty_rounded
    FROM joined_order_items
    WHERE stock_qty <= 337
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        running_count_stock_qty,
        running_avg_stock_qty,
        product_stock_qty,
        AVG(product_stock_qty) OVER (ORDER BY running_count_stock_qty ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_stock_qty_avg_3,
        MAX(running_count_stock_qty) OVER (PARTITION BY product_id) AS product_id_max_running_count_stock_qty,
        SUM(cost) OVER (PARTITION BY product_id ORDER BY running_count_stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM valid
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(running_cost * 1.07, 2) AS running_cost_adjusted
    FROM with_windows
    WHERE product_stock_qty_avg_3 < 280
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    CASE WHEN running_cost_adjusted >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_cost_adjusted_tier,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    MIN(quantity) AS min_quantity,
    ROUND(AVG(product_stock_qty_avg_3), 2) AS avg_product_stock_qty_avg_3,
    ROUND(AVG(quantity), 2) AS avg_quantity_2,
    ROUND(AVG(running_cost_adjusted), 2) AS avg_running_cost_adjusted
FROM valid_order_items
GROUP BY CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_cost_adjusted >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING BIT_AND(quantity) < 1000;
