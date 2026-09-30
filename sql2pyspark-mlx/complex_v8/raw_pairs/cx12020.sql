WITH recent_moves AS (
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
        CASE WHEN move_type IN ('INBOUND', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE operator <> 'op_ann'
)
SELECT warehouse_id, moved_at AS happened_at, move_type_group
FROM recent_moves
WHERE operator = 'op_eve'
  AND qty BETWEEN 90 AND 426
UNION
SELECT
    qty AS warehouse_id,
    moved_at AS happened_at,
    move_type AS move_type_group
FROM stock_moves
WHERE sku_id <= 18
  AND warehouse_id >= 4
