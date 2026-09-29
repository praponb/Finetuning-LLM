WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE qty < 387
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.move_type_len,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM filtered_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(qty, 2) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type, move_type_len), 0), 4) AS unit_price_share_of_move_type_move_type_len,
        ROUND(PERCENT_RANK() OVER w_all_by_moved_at_ties, 3) AS moved_at_percent_rank,
        ROUND(AVG(qty) OVER (PARTITION BY move_type, move_type_len), 2) AS move_type_move_type_len_avg_qty,
        LEAD(moved_at) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id) AS next_moved_at,
        NTH_VALUE(qty, 2) OVER (PARTITION BY move_type ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_2,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, warehouse_id ORDER BY sku_unit_cost DESC), 3) AS sku_unit_cost_cume_dist,
        SUM(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_sum_2,
        MAX(move_type_len) OVER w_sku_id AS sku_id_max_move_type_len,
        NTILE(5) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id) AS qty_quintile,
        MIN(move_type_len) OVER (PARTITION BY sku_id) AS sku_id_min_move_type_len
    FROM with_details_sku
    WINDOW w_all_by_moved_at_ties AS (ORDER BY moved_at DESC), w_sku_id AS (PARTITION BY sku_id)
)
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    move_type_len,
    sku_code,
    sku_labels,
    sku_unit_cost,
    second_qty,
    unit_price_share_of_move_type_move_type_len,
    moved_at_percent_rank,
    move_type_move_type_len_avg_qty,
    next_moved_at,
    second_qty_2,
    sku_unit_cost_cume_dist,
    qty_sum_2,
    sku_id_max_move_type_len,
    qty_quintile,
    CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
    CONCAT_WS(': ', move_type, sku_code) AS move_type_sku_code_label
FROM metrics
