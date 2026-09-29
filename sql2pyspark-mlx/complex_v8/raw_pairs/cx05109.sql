WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        operator,
        days_since_moved_at,
        ABS(unit_price - 206.8) AS unit_price_distance,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj
    FROM filtered_moves
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.unit_price_distance,
        st.id AS move_id,
        st.move_type,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM labeled s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
scored AS (
    -- Add calculated columns
    SELECT *, ROUND(unit_price_distance * 1.1, 2) AS unit_price_distance_adj
    FROM with_children
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        move_id,
        move_type,
        move_operator,
        SUM(unit_price_distance_adj) FILTER (WHERE move_type = 'INBOUND') AS unit_price_distance_adj_move_type_inbound,
        ROUND(AVG(unit_price_distance), 2) AS avg_unit_price_distance,
        SUM(unit_price_distance) AS total_unit_price_distance,
        SUM(unit_price_distance_adj) AS total_unit_price_distance_adj,
        COUNT(*) FILTER (WHERE move_operator <> 'op_dew') AS n_move_operator_op_dew,
        COUNT(*) AS cnt
    FROM scored
    GROUP BY move_id, move_type, move_operator
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE unit_price_distance_adj_move_type_inbound > (SELECT MIN(unit_price_distance_adj_move_type_inbound) FROM per_group)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, st.unit_price AS move_unit_price
    FROM filtered s
    LEFT JOIN stock_moves st ON st.id = s.move_id AND st.move_type = 'ADJUST'
)
SELECT
    move_id,
    move_operator,
    unit_price_distance_adj_move_type_inbound,
    total_unit_price_distance,
    total_unit_price_distance_adj,
    n_move_operator_op_dew,
    cnt,
    ROUND(move_unit_price * 1.07, 2) AS move_unit_price_adj,
    SUBSTRING(TRIM(move_operator), 1, 2) AS move_operator_prefix
FROM combined_details
ORDER BY move_unit_price_adj NULLS LAST
