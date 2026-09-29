WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE operator <> 'op_eve'
      AND warehouse_id <= 5
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator,
        st.qty AS move_qty
    FROM prepared_moves s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        move_sku_id,
        move_move_type,
        MIN(unit_price) AS min_unit_price,
        MAX(moved_at) AS last_moved_at,
        SUM(unit_price) FILTER (WHERE move_move_type NOT IN ('OUTBOUND', 'ADJUST')) AS unit_price_move_move_type_not_in_outbou,
        COUNT(*) AS n_rows,
        ROUND(SUM(unit_price) / NULLIF(SUM(qty), 0), 3) AS unit_price_to_qty_ratio,
        ROUND(100 * COUNT(CASE WHEN operator = 'op_chan' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_chan
    FROM detail_rows
    GROUP BY sku_id, move_sku_id, move_move_type
),
windowed AS (
    -- Previous / next values per key
    SELECT
        sku_id,
        move_sku_id,
        move_move_type,
        min_unit_price,
        last_moved_at,
        unit_price_move_move_type_not_in_outbou,
        n_rows,
        unit_price_to_qty_ratio,
        pct_operator_op_chan,
        SIZE(COLLECT_SET(sku_id) OVER w_move_sku_id) AS distinct_sku_id_in_move_sku_id,
        ROUND(min_unit_price / NULLIF(SUM(min_unit_price) OVER (PARTITION BY move_sku_id), 0), 4) AS min_unit_price_share_of_move_sku_id,
        ROUND(pct_operator_op_chan / NULLIF(SUM(pct_operator_op_chan) OVER (PARTITION BY move_sku_id), 0), 4) AS pct_operator_op_chan_share_of_move_sku_id,
        LEAD(unit_price_to_qty_ratio) OVER (PARTITION BY n_rows ORDER BY min_unit_price, sku_id, move_sku_id, move_move_type) AS next_unit_price_to_qty_ratio
    FROM grouped
    WINDOW w_move_sku_id AS (PARTITION BY move_sku_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY YEAR(last_moved_at), distinct_sku_id_in_move_sku_id), 0), 4) AS n_rows_share_of_last_moved_at_year_distinct_sku_,
        LAG(pct_operator_op_chan, 3) OVER w_move_sku_id_by_min_unit_pric AS prev3_pct_operator_op_chan,
        SUM(n_rows) OVER (PARTITION BY move_sku_id ORDER BY min_unit_price_share_of_move_sku_id DESC, sku_id, move_sku_id, move_move_type ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS n_rows_sum_7,
        ROUND(unit_price_to_qty_ratio / NULLIF(SUM(unit_price_to_qty_ratio) OVER (PARTITION BY YEAR(last_moved_at), distinct_sku_id_in_move_sku_id), 0), 4) AS unit_price_to_qty_ratio_share_of_last_moved_at_y,
        ROW_NUMBER() OVER (PARTITION BY move_sku_id ORDER BY min_unit_price_share_of_move_sku_id DESC, sku_id, move_sku_id, move_move_type) AS min_unit_price_share_of_move_sku_id_seq
    FROM windowed
    WINDOW w_move_sku_id_by_min_unit_pric AS (PARTITION BY move_sku_id ORDER BY min_unit_price_share_of_move_sku_id DESC, sku_id, move_sku_id, move_move_type)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        sku_id,
        move_sku_id,
        move_move_type,
        min_unit_price,
        last_moved_at,
        unit_price_move_move_type_not_in_outbou,
        n_rows,
        unit_price_to_qty_ratio,
        pct_operator_op_chan,
        distinct_sku_id_in_move_sku_id,
        min_unit_price_share_of_move_sku_id,
        pct_operator_op_chan_share_of_move_sku_id,
        next_unit_price_to_qty_ratio,
        n_rows_share_of_last_moved_at_year_distinct_sku_,
        prev3_pct_operator_op_chan,
        n_rows_sum_7,
        unit_price_to_qty_ratio_share_of_last_moved_at_y
    FROM sequenced
    WHERE min_unit_price_share_of_move_sku_id_seq <= 3
      AND move_move_type <> 'ADJUST'
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.sku_id,
        s.move_sku_id,
        s.move_move_type,
        s.min_unit_price,
        s.last_moved_at,
        s.unit_price_move_move_type_not_in_outbou,
        s.n_rows,
        s.unit_price_to_qty_ratio,
        s.pct_operator_op_chan,
        s.distinct_sku_id_in_move_sku_id,
        s.next_unit_price_to_qty_ratio,
        s.prev3_pct_operator_op_chan,
        s.n_rows_sum_7,
        s.unit_price_to_qty_ratio_share_of_last_moved_at_y,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg
    FROM first_rows s
    LEFT JOIN skus sk ON sk.id = s.move_sku_id
)
SELECT
    sku_id,
    move_sku_id,
    last_moved_at,
    unit_price_move_move_type_not_in_outbou,
    n_rows,
    unit_price_to_qty_ratio,
    pct_operator_op_chan,
    next_unit_price_to_qty_ratio,
    prev3_pct_operator_op_chan,
    n_rows_sum_7,
    unit_price_to_qty_ratio_share_of_last_moved_at_y,
    sku_labels,
    DATEDIFF(DATE '2025-12-31', last_moved_at) AS days_since_last_moved_at,
    SUBSTRING(TRIM(move_move_type), 1, 4) AS move_move_type_prefix
FROM combined_details
ORDER BY last_moved_at, n_rows DESC
