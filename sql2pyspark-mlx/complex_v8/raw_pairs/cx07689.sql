WITH cleaned_moves AS (
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
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE operator = 'op_chan'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        operator_prefix,
        move_type_slug,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type, sku_id), 0), 4) AS unit_price_share_of_move_type_sku_id,
        ROUND(AVG(qty) OVER (PARTITION BY move_type, sku_id), 2) AS move_type_sku_id_avg_qty
    FROM cleaned_moves
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty >= 169
        )
)
SELECT *
FROM (
    SELECT warehouse_id, moved_at, move_type, unit_price_share_of_move_type_sku_id
    FROM with_activity
) src
PIVOT (
    COUNT(unit_price_share_of_move_type_sku_id) AS count_v, SUM(unit_price_share_of_move_type_sku_id) AS sum_v
    FOR move_type IN ('ADJUST', 'INBOUND', 'OUTBOUND', 'TRANSFER')
)
