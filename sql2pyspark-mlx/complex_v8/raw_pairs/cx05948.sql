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
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 8 AND 14
      AND move_type NOT IN ('TRANSFER', 'OUTBOUND')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), operator ORDER BY moved_at, id) AS moved_at_rank,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty DESC, id) AS seq_no,
        ROUND(qty_filled / NULLIF(SUM(qty_filled) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_filled_share_of_warehouse_id
    FROM prepared
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, unit_price, qty_filled_share_of_warehouse_id
    FROM ranked
) src
UNPIVOT (
    amount FOR metric IN (unit_price AS m_unit_price, qty_filled_share_of_warehouse_id AS qty_filled_share_of_warehouse_id_v)
) unp
