WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE move_type <> 'ADJUST'
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM combined_details_warehouse s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
),
sku_id_stats AS (
    -- Avg unit_price per sku_id
    SELECT sku_id, AVG(unit_price) AS avg_unit_price
    FROM filtered
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM filtered s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price > g.avg_unit_price
)
-- Subtotals with GROUPING SETS
SELECT
    operator_or_unknown,
    sku_id,
    GROUPING(operator_or_unknown) AS is_total_operator_or_unknown,
    COUNT(*) AS row_count,
    MIN(moved_at) AS first_moved_at,
    COUNT(DISTINCT operator_or_unknown) AS distinct_operator_or_unknowns
FROM vs_group
GROUP BY GROUPING SETS ((operator_or_unknown, sku_id), (operator_or_unknown), (sku_id), ())
ORDER BY operator_or_unknown NULLS LAST, sku_id NULLS LAST
