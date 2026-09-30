WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled,
        LENGTH(TRIM(wh_code)) AS wh_code_len,
        LENGTH(TRIM(city)) AS city_len
    FROM warehouses
    WHERE city IN ('Bangkok', 'Singapore')
      AND UPPER(TRIM(wh_code)) LIKE 'B%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(capacity - AVG(capacity) OVER w_wh_code_len, 2) AS capacity_vs_wh_code_len_avg,
        ROUND(capacity_filled / NULLIF(SUM(capacity_filled) OVER (PARTITION BY wh_code_len), 0), 4) AS capacity_filled_share_of_wh_code_len,
        MAX(capacity_filled) OVER (ORDER BY city_len DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS capacity_filled_max_3_prior,
        MAX(capacity_filled) OVER w_wh_code_len AS wh_code_len_max_capacity_filled
    FROM prepared
    WINDOW w_wh_code_len AS (PARTITION BY wh_code_len)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE capacity_filled_max_3_prior < (SELECT MAX(capacity_filled_max_3_prior) FROM metrics)
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.opened_on,
        s.capacity_filled,
        s.wh_code_len,
        s.city_len,
        s.capacity_filled_share_of_wh_code_len,
        s.capacity_filled_max_3_prior,
        s.wh_code_len_max_capacity_filled,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id
    FROM qualified s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT s.*, sk.category AS sku_category, sk.labels AS sku_labels
    FROM with_children_moves s
    JOIN skus sk ON sk.id = s.move_sku_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        COUNT(CASE WHEN operator IN ('op_bee', 'op_chan', 'op_eve') THEN 1 END) AS move_operator_count,
        MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_operator_count, 0) AS move_operator_count,
        st.first_moved_at
    FROM with_parent_sku s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY wh_code_len, move_sku_id ORDER BY capacity_filled DESC, id, move_id) AS capacity_filled_quartile,
        COUNT(*) OVER w_wh_code_len_move_operator_co AS rows_in_wh_code_len_move_operator_count,
        ROUND(CUME_DIST() OVER w_wh_code_len_move_sku_id_by_c_2, 3) AS capacity_filled_cume_dist,
        RANK() OVER (PARTITION BY wh_code_len, move_sku_id ORDER BY capacity_filled DESC) AS capacity_filled_rank,
        ROUND(AVG(move_qty) OVER (PARTITION BY wh_code_len, move_operator_count), 2) AS wh_code_len_move_operator_count_avg_move_qty,
        FIRST_VALUE(move_operator_count) OVER (PARTITION BY wh_code_len, move_sku_id ORDER BY capacity_filled DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_move_operator_count
    FROM profiled
    WINDOW w_wh_code_len_move_operator_co AS (PARTITION BY wh_code_len, move_operator_count), w_wh_code_len_move_sku_id_by_c_2 AS (PARTITION BY wh_code_len, move_sku_id ORDER BY capacity_filled DESC)
)
SELECT id, move_id, metric_name, metric_value
FROM (
    SELECT id, move_id, capacity_filled_cume_dist, wh_code_len_move_operator_count_avg_move_qty, capacity_filled_share_of_wh_code_len
    FROM sequenced
) src
UNPIVOT (
    metric_value FOR metric_name IN (capacity_filled_cume_dist, wh_code_len_move_operator_count_avg_move_qty, capacity_filled_share_of_wh_code_len)
) unp
