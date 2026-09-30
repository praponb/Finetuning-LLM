WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(cost, price) AS least_cost_price
    FROM products
    WHERE brand IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY least_cost_price, id) AS row_seq,
        NTILE(10) OVER w_ordered AS stock_qty_bucket,
        MIN(price) OVER (PARTITION BY category, brand) AS category_min_price,
        SUM(cost) OVER w_grp AS cumulative_cost,
        SUM(least_cost_price) OVER (PARTITION BY supplier_id) AS supplier_id_sum_least_cost_price
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY brand ORDER BY stock_qty DESC, id), w_grp AS (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        stock_qty,
        tags,
        least_cost_price,
        stock_qty_bucket,
        LEAD(price, 2) OVER (PARTITION BY YEAR(launch_date), supplier_id ORDER BY supplier_id_sum_least_cost_price, id) AS next_price,
        SUM(stock_qty_bucket) OVER w_recent AS cumulative_stock_qty_bucket
    FROM enriched
    WHERE stock_qty_bucket <> 6
    WINDOW w_recent AS (PARTITION BY brand, supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (2, 3, 24, 35)
        )
)
SELECT
    id,
    brand,
    stock_qty,
    tags,
    stock_qty_bucket,
    next_price,
    cumulative_stock_qty_bucket
FROM eligible;
