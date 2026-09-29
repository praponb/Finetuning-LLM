WITH covered AS (
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
summary AS (
    -- Group-level totals
    SELECT
        prior_rows_1d,
        sku_id,
        SUM(prior_unit_price_1d) AS total_prior_unit_price_1d,
        ROUND(SUM(prior_unit_price_1d) / NULLIF(SUM(prior_rows_1d), 0), 3) AS prior_unit_price_1d_to_prior_rows_1d_ratio
    FROM covered
    GROUP BY 1, 2
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
    FROM summary s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    prior_rows_1d,
    sku_id,
    prior_unit_price_1d_to_prior_rows_1d_ratio,
    move_count,
    move_count_2,
    GREATEST(prior_rows_1d, move_count_2) AS greatest_prior_rows_1d_move_count_2,
    COALESCE(move_count_2, 0) AS move_count_2_filled
FROM with_stats
