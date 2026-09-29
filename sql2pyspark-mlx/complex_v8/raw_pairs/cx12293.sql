WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 13:00:00'
      AND move_type = 'ADJUST'
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.move_type_ref,
        w.wh_code AS warehouse_wh_code
    FROM prepared s
    JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        move_type_ref,
        warehouse_wh_code,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id)) AS distinct_move_type_in_sku_id,
        FIRST_VALUE(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROW_NUMBER() OVER w_all_by_qty AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty DESC, id) AS qty_seq,
        MIN(qty) OVER w_sku_id AS sku_id_min_qty,
        DENSE_RANK() OVER w_sku_id_by_qty_ties AS qty_dense_rank
    FROM joined
    WINDOW w_all_by_qty AS (ORDER BY qty DESC, id), w_sku_id AS (PARTITION BY sku_id), w_sku_id_by_qty_ties AS (PARTITION BY sku_id ORDER BY qty DESC NULLS FIRST)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE qty_seq = 1
      AND first_qty >= 139
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_qty, 2) OVER (PARTITION BY qty_seq, distinct_move_type_in_sku_id ORDER BY rows_in_sku_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_qty,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER w_warehouse_id, 0), 4) AS seq_no_share_of_warehouse_id,
        MAX(qty_seq) OVER (PARTITION BY qty_seq, distinct_move_type_in_sku_id ORDER BY rows_in_sku_id ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS qty_seq_max_3
    FROM top_rows
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT
    id,
    sku_id,
    move_type_ref,
    warehouse_wh_code,
    seq_no,
    qty_seq,
    second_first_qty,
    seq_no_share_of_warehouse_id,
    INITCAP(TRIM(move_type_ref)) AS move_type_ref_clean
FROM with_windows
ORDER BY second_first_qty
