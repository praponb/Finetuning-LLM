WITH in_window AS (
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
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_unit_price_1d,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost
    FROM in_window s
    JOIN skus sk ON sk.id = s.sku_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        sku_unit_cost + prior_unit_price_1d AS sku_unit_cost_plus_prior_unit_price_1d
    FROM with_details
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM enriched s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT *
FROM (
    SELECT sku_id, sku_category, prior_unit_price_1d
    FROM with_stats
) src
PIVOT (
    MIN(prior_unit_price_1d)
    FOR sku_category IN ('Apparel', 'Beauty', 'Toys')
)
ORDER BY sku_id
