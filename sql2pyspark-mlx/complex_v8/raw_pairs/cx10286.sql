WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_parent_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        sk.unit_cost AS sku_unit_cost,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM valid s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_parent_sku s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator <> 'op_dew'
              AND TO_DATE(st.moved_at) < DATE '2025-02-18'
        )
),
filtered AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity <= 11506
              AND YEAR(w.opened_on) = 2025
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(moved_at) AS moved_at_year,
        CASE WHEN sku_unit_cost >= 320.4 THEN 'large' WHEN sku_unit_cost >= 164.5 THEN 'medium' ELSE 'small' END AS sku_unit_cost_tier,
        operator,
        DATEDIFF(MAX(warehouse_opened_on), MIN(warehouse_opened_on)) AS warehouse_opened_on_span_days,
        COUNT(*) AS cnt,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
        ROUND(SUM(qty) / NULLIF(SUM(unit_price), 0), 3) AS qty_to_unit_price_ratio
    FROM filtered
    GROUP BY YEAR(moved_at), CASE WHEN sku_unit_cost >= 320.4 THEN 'large' WHEN sku_unit_cost >= 164.5 THEN 'medium' ELSE 'small' END, operator
),
filtered_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE warehouse_opened_on_span_days < (SELECT MAX(warehouse_opened_on_span_days) FROM stats)
)
SELECT *
FROM (
    SELECT sku_unit_cost_tier, cnt, operator, warehouse_opened_on_span_days
    FROM filtered_2
) src
PIVOT (
    COUNT(warehouse_opened_on_span_days)
    FOR operator IN ('op_eve', 'op_chan', 'op_bee', 'op_dew')
)
ORDER BY sku_unit_cost_tier, cnt
