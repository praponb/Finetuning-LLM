WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND unit_price < 372.6
),
latest_per_key AS (
    -- Keep the earliest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty, id) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_clean,
        w.wh_code AS warehouse_wh_code
    FROM latest_per_key s
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_clean,
        SUM(qty) OVER (ORDER BY moved_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at ASC NULLS LAST, id), 3) AS moved_at_percent_rank,
        MIN(qty) OVER (PARTITION BY sku_id) AS sku_id_min_qty,
        NTILE(5) OVER (ORDER BY qty, id) AS qty_quintile
    FROM with_parent_warehouse
)
SELECT
    operator,
    sku_id,
    warehouse_id,
    MIN(moved_at) AS first_moved_at
FROM windowed
GROUP BY GROUPING SETS ((operator, sku_id, warehouse_id), (operator), ())
ORDER BY operator NULLS LAST, sku_id NULLS LAST, warehouse_id NULLS LAST
