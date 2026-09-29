WITH filtered AS (
    -- Filter the raw rows and derive helper columns
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
        ARRAY_CONTAINS(tags, 'eco') AS is_eco,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE launch_date >= TO_DATE('2023-08-17')
      AND brand IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT category
    FROM filtered
    INTERSECT
    SELECT category
    FROM products
    WHERE supplier_id = 34
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.category = k.category
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.unit_price >= 330
        )
),
not_flagged_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date >= s.launch_date
        )
)
-- Join a dimension and aggregate
SELECT
    d.city,
    s.category,
    COUNT(*) AS cnt,
    ROUND(AVG(s.price), 2) AS avg_price,
    GROUPING_ID() AS gid
FROM not_flagged_products s
JOIN suppliers d ON s.supplier_id = d.id
GROUP BY ROLLUP (d.city, s.category)
ORDER BY city DESC NULLS LAST;
