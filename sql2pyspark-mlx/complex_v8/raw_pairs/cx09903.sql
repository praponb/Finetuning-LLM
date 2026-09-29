WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-08 12:00:00'
      AND sku_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at_date,
        operator_ref,
        SUBSTRING(TRIM(operator_ref), 1, 2) AS operator_ref_prefix
    FROM valid_moves
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at_date,
        s.operator_ref_prefix,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at,
        st.move_type
    FROM enriched s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY TO_DATE(move_moved_at), warehouse_id ORDER BY qty, id, move_id) AS qty_decile,
        COUNT(*) OVER (PARTITION BY move_operator) AS rows_in_move_operator,
        FIRST_VALUE(moved_at_date) IGNORE NULLS OVER (PARTITION BY TO_DATE(move_moved_at), warehouse_id ORDER BY qty, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_date,
        SUM(move_unit_price) OVER (PARTITION BY TO_DATE(move_moved_at), warehouse_id ORDER BY qty, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_unit_price,
        ROUND(move_unit_price / NULLIF(SUM(move_unit_price) OVER w_move_operator, 0), 4) AS move_unit_price_share_of_move_operator
    FROM with_children
    WINDOW w_move_operator AS (PARTITION BY move_operator)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DAYOFWEEK(move_moved_at) AS move_moved_at_dayofweek,
        CASE WHEN rows_in_move_operator >= 13 THEN 'large' WHEN rows_in_move_operator >= 7 THEN 'medium' ELSE 'small' END AS rows_in_move_operator_tier,
        CONCAT(move_type, '#', CAST(rows_in_move_operator AS STRING)) AS move_type_ref
    FROM ranked
    WHERE qty_decile > 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER w_all_by_move_unit_price AS move_unit_price_decile,
        SUM(move_moved_at_dayofweek) OVER (PARTITION BY MONTH(first_known_moved_at_date) ORDER BY move_unit_price ASC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_moved_at_dayofweek,
        SIZE(COLLECT_SET(move_operator) OVER w_qty_decile_move_id) AS distinct_move_operator_in_qty_decile_move_id,
        SUM(qty_decile) OVER w_qty_decile_move_id AS qty_decile_move_id_sum_qty_decile
    FROM tagged
    WINDOW w_all_by_move_unit_price AS (ORDER BY move_unit_price DESC, id, move_id), w_qty_decile_move_id AS (PARTITION BY qty_decile, move_id)
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at_date,
    operator_ref_prefix,
    move_id,
    move_moved_at,
    move_type,
    qty_decile,
    first_known_moved_at_date,
    move_unit_price_share_of_move_operator,
    move_moved_at_dayofweek,
    rows_in_move_operator_tier,
    move_type_ref,
    move_unit_price_decile,
    running_move_moved_at_dayofweek,
    distinct_move_operator_in_qty_decile_move_id,
    qty_decile_move_id_sum_qty_decile,
    LAST_DAY(moved_at_date) AS moved_at_date_month_end
FROM windowed
