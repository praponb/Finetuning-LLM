WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY operator ORDER BY unit_price ASC) AS unit_price_rank,
        days_since_moved_at - LAG(days_since_moved_at) OVER w_operator_by_unit_price AS days_since_moved_at_change
    FROM prepared_moves
    WINDOW w_operator_by_unit_price AS (PARTITION BY operator ORDER BY unit_price ASC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_id,
    unit_price_rank,
    GROUPING(warehouse_id) AS is_total_warehouse_id,
    ROUND(SUM(least_qty_unit_price) / NULLIF(SUM(days_since_moved_at_change), 0), 3) AS least_qty_unit_price_to_days_since_moved_at_chan,
    ROUND(SUM(unit_price) / NULLIF(SUM(days_since_moved_at_change), 0), 3) AS unit_price_to_days_since_moved_at_change_ratio
FROM windowed
GROUP BY GROUPING SETS ((warehouse_id, unit_price_rank), (warehouse_id), ())
ORDER BY warehouse_id NULLS LAST, unit_price_rank NULLS LAST
