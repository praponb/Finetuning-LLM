WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
    FROM stock_moves
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_plus_7d,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code,
        sk.unit_cost AS sku_unit_cost,
        sk.sku_code
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        sku_unit_cost - LAG(sku_unit_cost) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), operator ORDER BY warehouse_opened_on, id) AS sku_unit_cost_change,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), operator ORDER BY warehouse_opened_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sku_unit_cost_avg_3
    FROM joined_warehouse
)
SELECT
    operator,
    move_type,
    sku_id,
    ROUND(SUM(sku_unit_cost_change) / NULLIF(SUM(unit_price), 0), 3) AS sku_unit_cost_change_to_unit_price_ratio,
    COUNT(DISTINCT sku_id) AS distinct_sku_ids
FROM with_windows
GROUP BY GROUPING SETS ((operator, move_type, sku_id), (operator))
ORDER BY operator NULLS LAST, move_type NULLS LAST, sku_id NULLS LAST
