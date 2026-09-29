WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        HOUR(moved_at) AS moved_at_hour,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE qty < 300
      AND warehouse_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.moved_at_month,
        s.moved_at_hour,
        s.unit_price_plus_qty,
        w.wh_code AS warehouse_wh_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.moved_at_month,
        s.moved_at_hour,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at
    FROM with_parent s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty - AVG(qty) OVER w_moved_at_hour_move_id, 2) AS qty_vs_moved_at_hour_move_id_avg,
        FIRST_VALUE(moved_at_hour) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_month) ORDER BY move_qty DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_hour,
        NTILE(5) OVER (PARTITION BY moved_at_hour, move_id ORDER BY unit_price, id, move_id) AS unit_price_quintile,
        ROUND(AVG(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_month) ORDER BY move_qty DESC, id, move_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        FIRST_VALUE(move_moved_at) OVER (PARTITION BY moved_at_hour ORDER BY move_moved_at DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_move_moved_at
    FROM exploded_detail_moves
    WINDOW w_moved_at_hour_move_id AS (PARTITION BY moved_at_hour, move_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type,
        unit_price,
        moved_at_hour,
        move_id,
        move_sku_id,
        move_operator,
        move_moved_at,
        qty_vs_moved_at_hour_move_id_avg,
        first_moved_at_hour,
        unit_price_quintile,
        first_move_moved_at,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM sequenced
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        unit_price,
        move_id,
        move_moved_at,
        RANK() OVER (PARTITION BY move_operator, move_id ORDER BY move_moved_at NULLS LAST) AS move_moved_at_rank,
        SUM(moved_at_hour) OVER (PARTITION BY moved_at_hour ORDER BY qty_vs_moved_at_hour_move_id_avg, id, move_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS moved_at_hour_sum_1_prior,
        DENSE_RANK() OVER w_all_by_first_move_moved_at_t AS first_move_moved_at_dense_rank
    FROM labeled
    WINDOW w_all_by_first_move_moved_at_t AS (ORDER BY first_move_moved_at)
),
combined_details_move AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.unit_price,
        s.move_id,
        s.move_moved_at_rank,
        s.moved_at_hour_sum_1_prior,
        s.first_move_moved_at_dense_rank,
        st.moved_at AS move_moved_at_2,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id
    FROM ranked s
    JOIN stock_moves st ON st.id = s.move_id
)
SELECT
    id,
    unit_price,
    move_id,
    move_moved_at_rank,
    moved_at_hour_sum_1_prior,
    first_move_moved_at_dense_rank,
    move_moved_at_2,
    move_warehouse_id
FROM combined_details_move
ORDER BY move_moved_at_rank DESC
