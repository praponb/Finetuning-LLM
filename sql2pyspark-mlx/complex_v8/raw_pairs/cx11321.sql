WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.unit_price) AS prior_unit_price_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.prior_rows_1d,
        s.prior_unit_price_1d,
        sk.is_active AS sku_is_active,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM matched s
    JOIN skus sk ON sk.id = s.sku_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE operator NOT IN ('op_chan', 'op_eve')) AS move_operator_count,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count,
        COALESCE(st.move_count, 0) AS move_count
    FROM joined_sku s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
sku_category_stats AS (
    -- Avg move_operator_count per sku_category
    SELECT sku_category, AVG(move_operator_count) AS avg_move_operator_count
    FROM with_stats
    GROUP BY sku_category
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_operator_count,
        ROUND(s.move_operator_count - g.avg_move_operator_count, 2) AS move_operator_count_minus_avg
    FROM with_stats s
    JOIN sku_category_stats g ON g.sku_category = s.sku_category
    WHERE s.move_operator_count > g.avg_move_operator_count
)
SELECT id, field, value
FROM (
    SELECT id, move_count, move_operator_count
    FROM vs_group
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (move_count, move_operator_count)
) unp
