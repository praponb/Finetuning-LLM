WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        INITCAP(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE (moved_at < TIMESTAMP '2025-03-08 09:00:00' OR unit_price > 61.2)
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS max_move_qty
    FROM cleaned s
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_counts s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type IS NOT NULL
              AND st.qty < 387
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY unit_price DESC) AS unit_price_rank,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price
    FROM filtered
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        unit_price_rank,
        second_unit_price,
        MAX(unit_price) OVER (PARTITION BY move_type ORDER BY max_move_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        DENSE_RANK() OVER (PARTITION BY move_type ORDER BY max_move_qty DESC, id) AS max_move_qty_dense_rank,
        MAX(qty) OVER (PARTITION BY sku_id, unit_price_rank) AS sku_id_unit_price_rank_max_qty,
        LEAD(second_unit_price, 2, 0) OVER (PARTITION BY move_type ORDER BY max_move_qty DESC, id) AS next2_second_unit_price,
        MIN(qty) OVER (PARTITION BY sku_id, unit_price_rank) AS sku_id_unit_price_rank_min_qty,
        ROW_NUMBER() OVER w_move_type_by_max_move_qty AS max_move_qty_seq
    FROM with_windows
    WINDOW w_move_type_by_max_move_qty AS (PARTITION BY move_type ORDER BY max_move_qty DESC, id)
),
joined AS (
    -- Join warehouses
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        sk.category AS sku_category,
        sk.sku_code
    FROM windowed s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    qty,
    unit_price,
    unit_price_rank,
    second_unit_price,
    max_unit_price_so_far,
    max_move_qty_dense_rank,
    sku_id_unit_price_rank_max_qty,
    next2_second_unit_price,
    sku_id_unit_price_rank_min_qty,
    max_move_qty_seq,
    warehouse_capacity,
    sku_category,
    ROUND(unit_price / NULLIF(next2_second_unit_price, 0), 3) AS unit_price_per_next2_second_unit_price
FROM joined
ORDER BY id DESC, max_unit_price_so_far DESC
