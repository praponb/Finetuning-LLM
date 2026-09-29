WITH prepared_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CONCAT(wh_code, '#', CAST(id AS STRING)) AS wh_code_ref,
        SUBSTRING(TRIM(wh_code), 1, 3) AS wh_code_prefix,
        DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
      AND capacity < 11768
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator IN ('op_dew', 'op_chan')) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_operator_count
    FROM prepared_warehouses s
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.unit_price <= 289.7
              AND st.move_type IS NOT NULL
        )
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.moved_at < TIMESTAMP '2025-03-08 11:00:00'
        )
      AND s.opened_on < DATE '2025-02-14'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(days_since_opened_on, 0) AS days_since_opened_on_filled,
        ROUND(days_since_opened_on / NULLIF(capacity, 0), 3) AS days_since_opened_on_per_capacity,
        COALESCE(move_operator_count, 0) AS move_operator_count_filled
    FROM without_matches
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(move_operator_count_filled) OVER (PARTITION BY YEAR(opened_on), city ORDER BY move_operator_count_filled, id) AS next_move_operator_count_filled,
        LAG(opened_on, 2) OVER (PARTITION BY YEAR(opened_on), city ORDER BY move_operator_count_filled, id) AS prev2_opened_on
    FROM scored
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    opened_on,
    wh_code_ref,
    wh_code_prefix,
    days_since_opened_on_filled,
    days_since_opened_on_per_capacity,
    next_move_operator_count_filled,
    prev2_opened_on
FROM with_windows
WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
