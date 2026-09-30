WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
      AND move_type IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at), 3) AS moved_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY qty, id), 3) AS qty_percent_rank
    FROM base_moves
)
SELECT
    sku_id,
    warehouse_id,
    PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
FROM with_windows
GROUP BY GROUPING SETS ((sku_id, warehouse_id), (sku_id), (warehouse_id), ())
