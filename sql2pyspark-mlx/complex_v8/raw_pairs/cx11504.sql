WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 5
      AND HOUR(moved_at) BETWEEN 7 AND 13
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM cleaned
    WHERE qty >= 124
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM tagged s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    move_type,
    move_operator,
    operator,
    GROUPING(move_type) AS is_total_move_type,
    ARRAY_SORT(COLLECT_SET(move_operator)) AS move_operator_list,
    COUNT(DISTINCT operator) AS distinct_operators,
    SUM(CASE WHEN move_type <> 'OUTBOUND' THEN 1 ELSE 0 END) AS n_move_type_outbound
FROM detail_rows
GROUP BY GROUPING SETS ((move_type, move_operator, operator), (move_type), (move_operator), ())
ORDER BY move_type NULLS LAST, move_operator NULLS LAST, operator NULLS LAST
