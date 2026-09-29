WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(stock_qty, 0) AS stock_qty_filled,
        stock_qty - price AS stock_qty_minus_price,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE brand IS NOT NULL
      AND launch_date >= DATE '2023-06-10'
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.price,
        s.cost,
        s.launch_date,
        s.stock_qty_filled,
        x.inventory_last_restock
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (5, 30)
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        launch_date,
        AVG(price) OVER (ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS price_avg_5,
        LAST_VALUE(cost) IGNORE NULLS OVER (ORDER BY stock_qty_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        LAST_VALUE(price) IGNORE NULLS OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price
    FROM not_flagged
),
enriched_products AS (
    -- Running and moving aggregates
    SELECT
        id,
        launch_date,
        price_avg_5,
        last_known_cost,
        last_known_price,
        COUNT(price_avg_5) OVER w_hist AS running_count_price_avg_5,
        MIN(price_avg_5) OVER w_hist AS running_min_price_avg_5,
        AVG(price_avg_5) OVER w_hist AS running_avg_price_avg_5
    FROM enriched
    WHERE launch_date BETWEEN '2023-10-01' AND '2024-01-01'
    WINDOW w_hist AS (ORDER BY last_known_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        last_known_price,
        running_avg_price_avg_5,
        last_known_cost
    FROM enriched_products
) src
UNPIVOT (
    metric_value FOR metric IN (last_known_price, running_avg_price_avg_5, last_known_cost)
);
