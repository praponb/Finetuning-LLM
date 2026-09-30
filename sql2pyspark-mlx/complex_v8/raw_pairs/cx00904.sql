WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
    WHERE operator NOT IN ('op_chan', 'op_dew')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        ROUND(PERCENT_RANK() OVER w_all_by_moved_at_week_ties, 3) AS moved_at_week_percent_rank,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        LEAD(unit_price) OVER (ORDER BY moved_at_week DESC, id) AS next_unit_price,
        MIN(unit_price) OVER (ORDER BY moved_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far
    FROM scoped
    WINDOW w_all_by_moved_at_week_ties AS (ORDER BY moved_at_week DESC), w_warehouse_id AS (PARTITION BY warehouse_id)
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.min_unit_price_so_far,
        sk.labels AS sku_labels
    FROM with_trends s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        min_unit_price_so_far,
        sku_labels,
        MIN(unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_min_unit_price,
        min_unit_price_so_far - LAG(min_unit_price_so_far) OVER (PARTITION BY warehouse_id ORDER BY unit_price NULLS LAST, id) AS min_unit_price_so_far_change
    FROM with_parent
)
SELECT id, field, value
FROM (
    SELECT id, min_unit_price_so_far, min_unit_price_so_far_change
    FROM windowed
) src
UNPIVOT (
    value FOR field IN (min_unit_price_so_far, min_unit_price_so_far_change)
) unp
ORDER BY id, field
