WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(launch_date, 28) launch_date_plus_28d
    FROM products
    WHERE launch_date > DATE '2023-06-10'
      AND supplier_id IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        launch_date_plus_28d,
        LEAST(price, cost) least_price_cost,
        CASE WHEN category IN ('Home', 'Books') THEN 'Group A' ELSE 'Other' END category_group
    FROM filtered
    WHERE launch_date >= DATE '2023-06-23'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock <= s.launch_date
        )
),
eligible_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.store_id IN (1, 2, 4, 6)
        )
)
SELECT
    s.id,
    s.cost,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.launch_date_plus_28d,
    s.least_price_cost,
    s.category_group,
    (SELECT MAX(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS max_product_price
FROM eligible_products s
ORDER BY supplier_id NULLS LAST;
