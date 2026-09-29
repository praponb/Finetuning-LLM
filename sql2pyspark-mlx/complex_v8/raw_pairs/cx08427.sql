WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator <> 'op_ann'
      AND warehouse_id <= 5
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        LAG(unit_price, 2) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev2_unit_price,
        FIRST_VALUE(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type)) AS distinct_sku_id_in_move_type,
        RANK() OVER (PARTITION BY move_type ORDER BY unit_price) AS unit_price_rank,
        SUM(unit_price) OVER (PARTITION BY move_type) AS move_type_sum_unit_price,
        unit_price - LAG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price, id) AS unit_price_change
    FROM base
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        prev2_unit_price,
        first_unit_price,
        distinct_sku_id_in_move_type,
        move_type_sum_unit_price,
        unit_price_change
    FROM with_trends
    WHERE unit_price_rank <= 5
),
derived AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM leaders
    WHERE sku_id IS NOT NULL
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at
    FROM derived s
)
-- Final projection
SELECT DISTINCT unit_price
FROM profiled
WHERE move_count > 1
