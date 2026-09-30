WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        tags,
        COALESCE(product_name, 'unassigned') AS product_name_filled,
        UPPER(TRIM(product_name)) AS product_name_normalized,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE product_name IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(category, 1, 3) AS category_prefix
    FROM filtered
    WHERE product_name LIKE 'R%'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC NULLS FIRST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.store_id = 4
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date < '2023-09-20'
        )
)
SELECT
    id,
    product_name,
    cost,
    stock_qty,
    tags,
    brand_clean,
    category_prefix,
    rn
FROM kept
ORDER BY stock_qty NULLS FIRST, rn
