WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE unit_price <= 313.8
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        sku_is_active,
        UPPER(TRIM(move_type)) AS move_type_clean,
        YEAR(moved_at) AS moved_at_year
    FROM with_parent
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_unit_price,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM enriched s
)
SELECT
    move_type,
    sku_is_active,
    sku_id,
    GROUPING(move_type) AS is_total_move_type,
    SUM(CASE WHEN move_type NOT IN ('INBOUND', 'ADJUST') THEN move_count ELSE 0 END) AS move_count_move_type_not_in_inbound_adj,
    SUM(move_count) AS total_move_count,
    MIN(moved_at_year) AS min_moved_at_year
FROM with_counts
GROUP BY GROUPING SETS ((move_type, sku_is_active, sku_id), (move_type))
ORDER BY move_type NULLS LAST, sku_is_active NULLS LAST, sku_id NULLS LAST
