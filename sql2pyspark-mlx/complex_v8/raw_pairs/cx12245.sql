WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year,
        LENGTH(TRIM(operator)) AS operator_len,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid
    WHERE unit_price > (SELECT AVG(unit_price) FROM valid)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        unit_price,
        moved_at_year,
        operator_len,
        move_type_slug,
        CONCAT_WS('|', move_type_slug, operator) AS move_type_slug_operator_label,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        ROUND(unit_price / NULLIF(operator_len, 0), 3) AS unit_price_per_operator_len
    FROM with_activity
    WHERE operator NOT IN ('op_bee', 'op_dew')
)
SELECT *
FROM (
    SELECT moved_at_year, operator_len, move_type, unit_price
    FROM enriched
) src
PIVOT (
    MAX(unit_price)
    FOR move_type IN ('OUTBOUND', 'ADJUST', 'TRANSFER', 'INBOUND')
)
ORDER BY moved_at_year, operator_len
