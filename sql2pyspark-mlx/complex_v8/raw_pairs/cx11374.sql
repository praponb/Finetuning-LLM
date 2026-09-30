WITH scoped AS (
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
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE operator <> 'op_ann'
      AND move_type IS NOT NULL
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_or_unknown,
        sk.unit_cost AS sku_unit_cost
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        unit_price,
        move_type_or_unknown,
        sku_unit_cost,
        RANK() OVER (ORDER BY qty DESC) AS qty_rank,
        FIRST_VALUE(sku_unit_cost) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_unit_cost,
        SUM(qty) OVER (PARTITION BY operator ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        MAX(qty) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_max_qty,
        MIN(unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_min_unit_price,
        ROUND(PERCENT_RANK() OVER w_move_type_by_unit_price_ties, 3) AS unit_price_percent_rank,
        LEAD(unit_price, 3) OVER (PARTITION BY operator ORDER BY qty ASC, id) AS next3_unit_price,
        RANK() OVER (PARTITION BY move_type ORDER BY unit_price DESC) AS unit_price_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, move_type_or_unknown ORDER BY qty NULLS LAST, id), 3) AS qty_percent_rank,
        LEAD(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price DESC, id) AS next_unit_price,
        MAX(unit_price) OVER (PARTITION BY operator, move_type_or_unknown ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        COUNT(*) OVER w_sku_id_move_type AS rows_in_sku_id_move_type,
        COUNT(*) OVER (PARTITION BY sku_id, move_type_or_unknown) AS rows_in_sku_id_move_type_or_unknown,
        MAX(unit_price) OVER (PARTITION BY operator ORDER BY qty ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_max_3,
        LAG(qty) OVER (PARTITION BY operator ORDER BY qty ASC, id) AS prev_qty,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id, move_type), 0), 4) AS unit_price_share_of_sku_id_move_type
    FROM with_details_sku
    WINDOW w_move_type_by_unit_price_ties AS (PARTITION BY move_type ORDER BY unit_price DESC), w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        unit_price,
        move_type_or_unknown,
        qty_rank,
        first_sku_unit_cost,
        sku_id_move_type_max_qty,
        sku_id_move_type_min_unit_price,
        next3_unit_price,
        qty_percent_rank,
        next_unit_price,
        rows_in_sku_id_move_type,
        rows_in_sku_id_move_type_or_unknown,
        prev_qty,
        unit_price_share_of_sku_id_move_type,
        RANK() OVER (ORDER BY unit_price_max_3 DESC) AS unit_price_max_3_rank,
        ROW_NUMBER() OVER (ORDER BY unit_price_rank, id) AS unit_price_rank_seq,
        ROUND(qty_percent_rank / NULLIF(SUM(qty_percent_rank) OVER (PARTITION BY unit_price_rank, move_type), 0), 4) AS qty_percent_rank_share_of_unit_price_rank_move_t,
        ROUND(qty_percent_rank - AVG(qty_percent_rank) OVER (PARTITION BY warehouse_id, move_type_or_unknown), 2) AS qty_percent_rank_vs_warehouse_id_move_type_or_un,
        LEAD(max_unit_price_so_far) OVER (ORDER BY sku_id_move_type_max_qty DESC, id) AS next_max_unit_price_so_far,
        MAX(unit_price) OVER (ORDER BY unit_price_rank, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_max_2,
        NTILE(10) OVER w_all_by_sku_id_move_type_max AS sku_id_move_type_max_qty_decile,
        ROUND(max_unit_price_so_far - AVG(max_unit_price_so_far) OVER (PARTITION BY unit_price_rank, move_type), 2) AS max_unit_price_so_far_vs_unit_price_rank_move_ty
    FROM ranked
    WINDOW w_all_by_sku_id_move_type_max AS (ORDER BY sku_id_move_type_max_qty DESC, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    unit_price,
    move_type_or_unknown,
    first_sku_unit_cost,
    sku_id_move_type_min_unit_price,
    next3_unit_price,
    qty_percent_rank,
    next_unit_price,
    rows_in_sku_id_move_type,
    prev_qty,
    unit_price_max_3_rank,
    unit_price_rank_seq,
    qty_percent_rank_share_of_unit_price_rank_move_t,
    qty_percent_rank_vs_warehouse_id_move_type_or_un,
    unit_price_max_2,
    sku_id_move_type_max_qty_decile,
    max_unit_price_so_far_vs_unit_price_rank_move_ty
FROM windowed
WHERE next_unit_price BETWEEN 112.8 AND 313.8
