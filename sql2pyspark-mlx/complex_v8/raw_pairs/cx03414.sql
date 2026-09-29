WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_operator_by_unit_price AS unit_price_seq,
        LEAD(unit_price_filled, 2, 0) OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST, id) AS next2_unit_price_filled,
        NTH_VALUE(unit_price_filled, 2) OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_filled,
        LEAD(unit_price, 1, 0) OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST, id) AS next_unit_price
    FROM prepared
    WINDOW w_operator_by_unit_price AS (PARTITION BY operator ORDER BY unit_price NULLS LAST, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(move_type)) AS move_type_clean,
        UPPER(TRIM(operator)) AS operator_clean
    FROM with_windows
    WHERE unit_price >= 189.4
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        MIN(moved_at) AS first_moved_at,
        MIN(qty) AS min_move_qty
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.first_moved_at,
        st.min_move_qty
    FROM annotated s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
SELECT
    unit_price_seq,
    move_count,
    sku_id,
    COUNT(DISTINCT operator) AS distinct_operators
FROM summarized_rows
GROUP BY CUBE(unit_price_seq, move_count, sku_id)
ORDER BY unit_price_seq NULLS LAST, move_count NULLS LAST, sku_id NULLS LAST
