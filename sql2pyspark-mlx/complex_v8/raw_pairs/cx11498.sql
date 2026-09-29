WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-25 07:00:00'
      AND operator IN ('op_bee', 'op_ann')
),
latest AS (
    -- Keep the latest row per operator, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(moved_at) OVER w_sku_id_moved_at_dayofweek_by AS prev_moved_at,
        ROW_NUMBER() OVER (PARTITION BY sku_id, moved_at_dayofweek ORDER BY unit_price ASC, id) AS unit_price_seq
    FROM latest
    WINDOW w_sku_id_moved_at_dayofweek_by AS (PARTITION BY sku_id, moved_at_dayofweek ORDER BY unit_price ASC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        moved_at_dayofweek,
        prev_moved_at,
        unit_price_seq,
        CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM ranked
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, SUM(qty) AS total_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_qty
    FROM tagged s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        moved_at_dayofweek,
        prev_moved_at,
        unit_price_seq,
        move_type_group,
        total_move_qty,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        MAX(unit_price_seq) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price_seq,
        FIRST_VALUE(total_move_qty) IGNORE NULLS OVER (PARTITION BY unit_price_seq ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_move_qty
    FROM with_stats
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        unit_price_seq,
        ROUND(SUM(total_move_qty) / NULLIF(SUM(first_known_total_move_qty), 0), 3) AS total_move_qty_to_first_known_total_move_qty_rat,
        COUNT(*) AS n_rows,
        MAX(total_move_qty) AS max_total_move_qty,
        SUM(unit_price_seq) AS total_unit_price_seq,
        MIN(first_known_total_move_qty) AS min_first_known_total_move_qty
    FROM with_windows
    WHERE first_known_total_move_qty < 1650
    GROUP BY unit_price_seq
)
SELECT
    unit_price_seq,
    n_rows,
    max_total_move_qty,
    min_first_known_total_move_qty,
    ABS(max_total_move_qty - 1650) AS max_total_move_qty_distance
FROM summary
ORDER BY unit_price_seq DESC, n_rows
