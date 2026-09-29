WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type NOT IN ('INBOUND', 'TRANSFER')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY unit_price DESC, id) AS seq_no,
        MIN(unit_price) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_min_unit_price,
        NTILE(10) OVER (PARTITION BY operator ORDER BY qty DESC, id) AS qty_decile,
        FIRST_VALUE(qty) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty
    FROM scoped_moves
),
latest AS (
    -- Keep the latest row per warehouse_id, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        seq_no,
        sku_id_operator_min_unit_price,
        qty_decile,
        first_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price DESC, id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        seq_no,
        qty_decile,
        first_qty,
        ROUND(PERCENT_RANK() OVER w_sku_id_seq_no_by_unit_price, 3) AS unit_price_percent_rank,
        LEAD(first_qty) OVER w_warehouse_id_by_moved_at AS next_first_qty,
        SUM(first_qty) OVER (PARTITION BY seq_no) AS seq_no_sum_first_qty,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id) AS moved_at_seq,
        COUNT(*) OVER (PARTITION BY seq_no) AS rows_in_seq_no
    FROM latest
    WINDOW w_sku_id_seq_no_by_unit_price AS (PARTITION BY sku_id, seq_no ORDER BY unit_price DESC), w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at DESC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(seq_no_sum_first_qty, seq_no) AS greatest_seq_no_sum_first_qty_seq_no
    FROM ranked
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.seq_no,
        s.qty_decile,
        s.first_qty,
        s.unit_price_percent_rank,
        s.seq_no_sum_first_qty,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id
    FROM labeled s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.qty_decile,
        s.first_qty,
        s.unit_price_percent_rank,
        s.seq_no_sum_first_qty,
        s.move_id,
        s.move_qty,
        s.move_operator,
        s.move_sku_id,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category,
        st.moved_at AS move_moved_at
    FROM with_children s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN stock_moves st ON st.id = s.move_id
)
SELECT *
FROM (
    SELECT sku_id, move_operator, qty_decile
    FROM with_parent_sku
) src
PIVOT (
    MAX(qty_decile) AS max_v, COUNT(qty_decile) AS n
    FOR move_operator IN ('op_ann', 'op_bee', 'op_chan')
)
ORDER BY sku_id
