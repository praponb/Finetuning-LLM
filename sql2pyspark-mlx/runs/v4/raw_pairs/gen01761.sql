WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        SUBSTRING(product_name, 1, 3) AS product_name_prefix
    FROM products
    WHERE launch_date > DATE '2023-05-05'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity = 5
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM eligible
    EXCEPT
    SELECT supplier_id
    FROM products
    WHERE price < 330
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id
)
SELECT
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    CASE WHEN category RLIKE '^(Books|Home)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS category_zone
FROM reconciled
ORDER BY stock_qty DESC, brand DESC
