WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        s.city,
        src.category,
        COUNT(*) AS cnt,
        MAX(src.stock_qty) AS max_stock_qty,
        MIN(src.cost) AS min_cost,
        GROUPING_ID() AS level_id
    FROM products src
    JOIN suppliers s ON src.supplier_id = s.id
    GROUP BY ROLLUP (s.city, src.category)
)
SELECT
    category,
    max_stock_qty,
    min_cost,
    level_id,
    CASE WHEN category RLIKE '^(Home|Books)$' THEN 'PRIORITY' WHEN category RLIKE '^(Electronics|Sports)$' THEN 'GROUP_A' ELSE 'CORE' END AS category_zone
FROM rollup_totals
