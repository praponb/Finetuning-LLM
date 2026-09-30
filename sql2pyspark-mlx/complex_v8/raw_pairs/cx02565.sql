WITH cleaned AS (
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
        CASE WHEN operator IN ('op_eve', 'op_bee') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
        LENGTH(TRIM(move_type)) AS move_type_len,
        INITCAP(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE (warehouse_id IS NOT NULL OR MONTH(moved_at) IN (1, 2))
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE unit_price > (SELECT AVG(unit_price) FROM cleaned)
),
latest_per_key AS (
    -- Keep the latest row per move_type, operator_group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_group,
        move_type_len,
        move_type_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, operator_group ORDER BY unit_price DESC, id DESC) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
)
SELECT id, metric, metric_value
FROM (
    SELECT id, qty, move_type_len
    FROM latest_per_key
) src
UNPIVOT (
    metric_value FOR metric IN (qty AS qty_v, move_type_len AS move_type_len_v)
) unp
