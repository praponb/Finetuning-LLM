WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(category) AS category_lower,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE brand IS NOT NULL
      AND price BETWEEN 370 AND 530
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock BETWEEN '2024-08-20' AND '2024-12-16'
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        price,
        cost,
        MIN(price) OVER (PARTITION BY brand) AS brand_min_price,
        ROUND(price / SUM(price) OVER (PARTITION BY MONTH(launch_date), supplier_id), 4) AS price_share,
        LAST_VALUE(price, TRUE) OVER (PARTITION BY MONTH(launch_date) ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price,
        SUM(stock_qty) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty
    FROM eligible
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.unit_price
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered_products AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(quantity, cost) AS greatest_quantity_cost,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM with_latest
    WHERE brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Stark'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN greatest_quantity_cost >= 72 THEN 'HIGH' ELSE 'LOW' END AS greatest_quantity_cost_tier,
        CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
        PERCENTILE_APPROX(cost, 0.95) AS cost_p95,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        GROUPING(CASE WHEN greatest_quantity_cost >= 72 THEN 'HIGH' ELSE 'LOW' END) AS gid
    FROM filtered_products
    GROUP BY CUBE (CASE WHEN greatest_quantity_cost >= 72 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END)
    HAVING avg_unit_price > 0
)
SELECT
    greatest_quantity_cost_tier,
    quantity_tier,
    cost_p95,
    avg_unit_price,
    gid
FROM summary
ORDER BY gid DESC;
