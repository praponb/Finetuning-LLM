WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tags,
        supplier_id,
        AVG(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
        SUM(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        COUNT(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost
    FROM products
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY running_count_cost DESC NULLS LAST) AS rn
    FROM with_windows
    QUALIFY rn = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.revenue >= 1800
        )
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.revenue
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.tags,
    s.supplier_id,
    s.running_sum_cost,
    (SELECT MIN(x.cost) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_cost
FROM with_latest s
