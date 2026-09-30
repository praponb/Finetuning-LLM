WITH prepared AS (
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
        COALESCE(qty, 0) AS qty_filled,
        COALESCE(qty, 0) AS qty_filled_2,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE sku_id >= 5
),
latest_per_key AS (
    -- Keep the earliest row per sku_id, operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_filled,
        qty_filled_2,
        operator_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY qty, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
)
SELECT
    warehouse_id,
    move_type,
    SUM(qty_filled_2) AS total_qty_filled_2
FROM latest_per_key
GROUP BY GROUPING SETS ((warehouse_id, move_type), (warehouse_id))
ORDER BY warehouse_id NULLS LAST, move_type NULLS LAST
