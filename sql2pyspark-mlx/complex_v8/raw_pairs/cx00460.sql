WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        QUARTER(moved_at) AS moved_at_quarter,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE warehouse_id <= 2
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(moved_at) AS moved_at_date,
        LEAST(least_qty_unit_price, qty) AS least_least_qty_unit_price_qty,
        CONCAT_WS('|', operator, operator_prefix) AS operator_operator_prefix_label
    FROM recent
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count, COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM enriched s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
latest_per_key AS (
    -- Keep the latest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        moved_at_quarter,
        operator_prefix,
        least_qty_unit_price,
        moved_at_date,
        least_least_qty_unit_price_qty,
        operator_operator_prefix_label,
        move_count,
        move_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY least_least_qty_unit_price_qty DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    moved_at_quarter,
    move_count_2,
    move_count,
    COUNT(*) AS n_rows,
    ROUND(SUM(least_qty_unit_price) / NULLIF(SUM(qty), 0), 3) AS least_qty_unit_price_to_qty_ratio,
    PERCENTILE_APPROX(least_least_qty_unit_price_qty, 0.5) AS median_least_least_qty_unit_price_qty
FROM latest_per_key
GROUP BY GROUPING SETS ((moved_at_quarter, move_count_2, move_count), (moved_at_quarter))
ORDER BY moved_at_quarter NULLS LAST, move_count_2 NULLS LAST, move_count NULLS LAST
