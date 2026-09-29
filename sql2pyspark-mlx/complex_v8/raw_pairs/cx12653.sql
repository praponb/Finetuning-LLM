WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator
    FROM stock_moves
    WHERE unit_price <= 330.9
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(move_type)) AS move_type_clean,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label_2
    FROM filtered
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        move_type_clean,
        QUARTER(moved_at) AS moved_at_quarter
    FROM enriched
    WHERE operator IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_type, operator, moved_at_quarter
    FROM annotated
) src
PIVOT (
    SUM(moved_at_quarter)
    FOR operator IN ('op_chan', 'op_bee')
)
