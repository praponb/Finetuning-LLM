WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.unit_price) AS prior_unit_price_3d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_3d,
        s.prior_unit_price_3d,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg
    FROM joined_ranges s
    JOIN skus sk ON sk.id = s.sku_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        SUM(unit_price) AS total_move_unit_price,
        COUNT(CASE WHEN move_type = 'INBOUND' THEN 1 END) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_unit_price,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM combined_details s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    id,
    moved_at,
    sku_unit_cost,
    sku_labels,
    sku_weight_kg,
    total_move_unit_price,
    move_move_type_count
FROM profiled
ORDER BY sku_weight_kg DESC, move_move_type_count NULLS LAST
