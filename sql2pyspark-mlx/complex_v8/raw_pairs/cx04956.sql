WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_move_type AS rows_in_move_type,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type)) AS distinct_sku_id_in_move_type,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_2
    FROM base_moves
    WINDOW w_move_type AS (PARTITION BY move_type)
),
latest AS (
    -- Keep the earliest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        unit_price_adj,
        least_unit_price_qty,
        rows_in_move_type,
        distinct_sku_id_in_move_type,
        unit_price_avg_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY unit_price_avg_2, id) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator NOT IN ('op_chan', 'op_eve')
        )
)
SELECT *
FROM (
    SELECT moved_at, move_type, operator, unit_price
    FROM qualified
) src
PIVOT (
    COUNT(unit_price)
    FOR operator IN ('op_dew', 'op_bee', 'op_eve', 'op_chan')
)
