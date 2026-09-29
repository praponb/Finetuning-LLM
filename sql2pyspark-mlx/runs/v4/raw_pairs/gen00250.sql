WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id IN (3, 16)
        )
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        quantity,
        ROUND(price / SUM(price) OVER (PARTITION BY category, supplier_id), 4) AS price_share,
        LAG(stock_qty, 2, 0) OVER (PARTITION BY brand ORDER BY cost, id) AS prev_stock_qty,
        SUM(price) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        MIN(quantity) OVER (PARTITION BY supplier_id, category) AS supplier_id_min_quantity,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost NULLS LAST) AS cost_dense_rank
    FROM latest_match
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        supplier_id,
        cost,
        price,
        price_share
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (cost, price, price_share)
);
