WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at_hour DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_max_3_prior,
        ROUND(AVG(qty) OVER (PARTITION BY operator, warehouse_id), 2) AS operator_warehouse_id_avg_qty,
        NTILE(5) OVER (ORDER BY moved_at DESC, id) AS moved_at_quintile,
        LAST_VALUE(moved_at) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at
    FROM filtered
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.qty_max_3_prior,
        s.operator_warehouse_id_avg_qty,
        s.last_moved_at,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM windowed s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
enriched_sku AS (
    -- Join skus
    SELECT s.*, sk.weight_kg AS sku_weight_kg
    FROM detail_rows s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        qty,
        operator,
        qty_max_3_prior,
        operator_warehouse_id_avg_qty,
        move_id,
        move_operator,
        sku_weight_kg,
        ROUND(PERCENT_RANK() OVER w_all_by_last_moved_at, 3) AS last_moved_at_percent_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_operator_operator) AS distinct_warehouse_id_in_move_operator_operator,
        NTILE(3) OVER w_all_by_last_moved_at AS last_moved_at_tercile,
        DENSE_RANK() OVER w_all_by_last_moved_at_ties AS last_moved_at_dense_rank,
        LEAD(last_moved_at) OVER (ORDER BY last_moved_at DESC, id, move_id) AS next_last_moved_at
    FROM enriched_sku
    WINDOW w_all_by_last_moved_at AS (ORDER BY last_moved_at DESC, id, move_id), w_move_operator_operator AS (PARTITION BY move_operator, operator), w_all_by_last_moved_at_ties AS (ORDER BY last_moved_at DESC)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        DATE_FORMAT(next_last_moved_at, 'yyyy-MM-dd HH:00') AS next_last_moved_at_label,
        ROUND(qty / NULLIF(sku_weight_kg, 0), 3) AS qty_per_sku_weight_kg
    FROM sequenced
    WHERE move_id <= 59
)
SELECT
    id,
    sku_id,
    qty,
    operator,
    qty_max_3_prior,
    operator_warehouse_id_avg_qty,
    move_id,
    move_operator,
    sku_weight_kg,
    last_moved_at_percent_rank,
    distinct_warehouse_id_in_move_operator_operator,
    last_moved_at_tercile,
    last_moved_at_dense_rank,
    next_last_moved_at,
    operator_slug,
    next_last_moved_at_label,
    qty_per_sku_weight_kg
FROM scored
ORDER BY distinct_warehouse_id_in_move_operator_operator DESC NULLS FIRST
