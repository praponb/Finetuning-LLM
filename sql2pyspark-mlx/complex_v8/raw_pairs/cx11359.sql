WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE qty BETWEEN 91 AND 463
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price_filled,
        st.id AS move_id,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id,
        st.move_type AS move_move_type,
        st.unit_price AS move_unit_price
    FROM base_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price_filled,
        move_id,
        move_operator,
        move_move_type,
        SUM(move_unit_price) OVER (PARTITION BY move_operator ORDER BY qty ASC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_unit_price,
        NTILE(4) OVER w_all_by_move_unit_price AS move_unit_price_quartile
    FROM exploded_detail_moves
    WINDOW w_all_by_move_unit_price AS (ORDER BY move_unit_price, id, move_id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY unit_price_filled, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        MIN(qty) OVER (PARTITION BY move_unit_price_quartile, sku_id) AS move_unit_price_quartile_sku_id_min_qty
    FROM sequenced
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        move_id,
        move_operator,
        move_move_type,
        running_move_unit_price,
        move_unit_price_quartile,
        move_unit_price_quartile_sku_id_min_qty,
        COUNT(*) OVER (PARTITION BY move_move_type, sku_id) AS rows_in_move_move_type_sku_id,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY moved_at, id, move_id), 3) AS moved_at_cume_dist,
        MAX(running_move_unit_price) OVER (PARTITION BY move_move_type, sku_id) AS move_move_type_sku_id_max_running_move_unit_pric
    FROM with_windows
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        moved_at,
        move_id,
        move_move_type,
        running_move_unit_price,
        move_unit_price_quartile,
        move_unit_price_quartile_sku_id_min_qty,
        rows_in_move_move_type_sku_id,
        moved_at_cume_dist,
        move_move_type_sku_id_max_running_move_unit_pric,
        RANK() OVER w_move_unit_price_quartile_mov AS move_unit_price_quartile_rank,
        LAST_VALUE(move_unit_price_quartile) OVER (PARTITION BY move_unit_price_quartile, move_move_type ORDER BY move_unit_price_quartile ASC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_move_unit_price_quartile,
        RANK() OVER (PARTITION BY move_unit_price_quartile, move_move_type ORDER BY move_unit_price_quartile ASC) AS move_unit_price_quartile_rank_2
    FROM with_windows_2
    WINDOW w_move_unit_price_quartile_mov AS (PARTITION BY move_unit_price_quartile, move_move_type ORDER BY move_unit_price_quartile ASC, id, move_id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.move_id,
        s.move_move_type,
        s.running_move_unit_price,
        s.move_unit_price_quartile,
        s.move_unit_price_quartile_sku_id_min_qty,
        s.rows_in_move_move_type_sku_id,
        s.move_unit_price_quartile_rank,
        s.last_move_unit_price_quartile,
        s.move_unit_price_quartile_rank_2,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost
    FROM with_trends s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    move_move_type,
    running_move_unit_price,
    move_unit_price_quartile,
    move_unit_price_quartile_sku_id_min_qty,
    last_move_unit_price_quartile,
    sku_weight_kg,
    sku_unit_cost,
    LENGTH(TRIM(move_move_type)) AS move_move_type_len
FROM combined_details
ORDER BY sku_id DESC NULLS FIRST
