WITH valid AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 12
      AND operator = 'op_chan'
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM valid s
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        unit_price,
        move_count,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM profiled
    WHERE qty < 372
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(unit_price) OVER (PARTITION BY move_count) AS move_count_min_unit_price,
        SUM(unit_price) OVER (PARTITION BY move_count) AS move_count_sum_unit_price
    FROM labeled
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    move_count_min_unit_price,
    COALESCE(unit_price, 0.0) AS unit_price_filled
FROM sequenced
WHERE unit_price < 387.1
ORDER BY move_count_min_unit_price DESC, id DESC
