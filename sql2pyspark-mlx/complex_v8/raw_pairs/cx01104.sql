WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND move_type IN ('TRANSFER', 'OUTBOUND')
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM scoped_moves
    ) ranked
    WHERE recency_rank = 1
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM latest s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT DISTINCT move_qty, sku_id, qty
FROM detail_rows
WHERE warehouse_id <= 2
ORDER BY move_qty, sku_id DESC
