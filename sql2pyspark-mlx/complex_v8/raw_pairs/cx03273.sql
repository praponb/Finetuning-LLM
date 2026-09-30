WITH recent AS (
    -- Filter stock_moves
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
    WHERE TO_DATE(moved_at) < DATE '2025-03-02'
      AND move_type = 'INBOUND'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type, warehouse_id ORDER BY qty ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_3,
        MIN(qty) OVER w_sku_id_move_type AS sku_id_move_type_min_qty,
        COUNT(*) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (ORDER BY unit_price, id) AS seq_no
    FROM recent
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
),
tagged AS (
    -- Add calculated columns
    SELECT *, COALESCE(unit_price_avg_3, 0.0) AS unit_price_avg_3_filled
    FROM windowed
),
leaders AS (
    -- First row per group
    SELECT *
    FROM tagged
    WHERE seq_no = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT seq_no, warehouse_id, move_type, running_count
    FROM leaders
) src
PIVOT (
    SUM(running_count)
    FOR move_type IN ('OUTBOUND', 'INBOUND')
)
ORDER BY seq_no, warehouse_id
