WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        CASE WHEN move_type IN ('ADJUST', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 13:00:00'
      AND sku_id IS NOT NULL
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty <= 275
        )
),
enriched AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.operator_slug,
        s.move_type_group,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on,
        sk.category AS sku_category,
        sk.sku_code
    FROM without_matches s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.capacity <= 11506
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IS NOT NULL
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        operator,
        COUNT(DISTINCT move_type_group) AS distinct_move_type_groups,
        COUNT(*) FILTER (WHERE sku_category IN ('Toys', 'Electronics', 'Apparel')) AS n_sku_category_in_toys_electro
    FROM filtered
    WHERE sku_category <> 'Toys'
    GROUP BY operator
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE distinct_move_type_groups > (SELECT AVG(distinct_move_type_groups) FROM rollup_base)
)
SELECT
    operator,
    distinct_move_type_groups,
    n_sku_category_in_toys_electro,
    SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM with_activity
ORDER BY distinct_move_type_groups DESC
