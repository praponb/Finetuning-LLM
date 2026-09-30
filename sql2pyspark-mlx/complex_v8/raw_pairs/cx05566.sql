WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug,
        COALESCE(capacity, 0) AS capacity_filled,
        SUBSTRING(TRIM(wh_code), 1, 4) AS wh_code_prefix
    FROM warehouses
    WHERE opened_on >= DATE '2024-04-13'
      AND capacity >= 4912
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.capacity,
        s.wh_code_prefix,
        st.id AS move_id,
        st.move_type,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM scoped s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_move_operator_move_type_by_m AS move_unit_price_dense_rank,
        NTH_VALUE(move_unit_price, 2) OVER (PARTITION BY move_operator, move_type ORDER BY move_unit_price DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_unit_price,
        ROUND(move_unit_price - AVG(move_unit_price) OVER w_move_operator, 2) AS move_unit_price_vs_move_operator_avg,
        FIRST_VALUE(capacity) IGNORE NULLS OVER (PARTITION BY move_operator, move_type ORDER BY move_unit_price DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_capacity
    FROM line_level_moves
    WINDOW w_move_operator_move_type_by_m AS (PARTITION BY move_operator, move_type ORDER BY move_unit_price DESC), w_move_operator AS (PARTITION BY move_operator)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(move_operator, 'UNKNOWN') AS move_operator_or_unknown,
        ABS(capacity - 11506) AS capacity_distance,
        ROUND(second_move_unit_price / NULLIF(first_known_capacity, 0), 3) AS second_move_unit_price_per_first_known_capacity
    FROM ranked
),
latest AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        wh_code,
        capacity,
        wh_code_prefix,
        move_id,
        move_type,
        move_unit_price,
        move_operator,
        move_unit_price_dense_rank,
        second_move_unit_price,
        move_unit_price_vs_move_operator_avg,
        first_known_capacity,
        move_operator_or_unknown,
        capacity_distance,
        second_move_unit_price_per_first_known_capacity
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY move_unit_price DESC, id DESC, move_id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    wh_code,
    wh_code_prefix,
    move_id,
    move_unit_price,
    move_operator,
    move_unit_price_dense_rank,
    move_unit_price_vs_move_operator_avg,
    capacity_distance,
    second_move_unit_price_per_first_known_capacity,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
FROM latest
ORDER BY wh_code_prefix DESC NULLS FIRST, move_type_or_unknown
