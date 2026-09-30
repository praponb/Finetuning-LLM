WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.unit_price) AS prior_unit_price_12h
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.sku_id, a.moved_at
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_12h,
        s.prior_unit_price_12h,
        sk.is_active AS sku_is_active,
        sk.category AS sku_category
    FROM in_window s
    JOIN skus sk ON sk.id = s.sku_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM joined_sku s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IN ('op_dew', 'op_bee')
        )
)
SELECT
    id,
    sku_id,
    prior_rows_12h,
    prior_unit_price_12h,
    sku_is_active,
    ROUND(prior_rows_12h / NULLIF(prior_unit_price_12h, 0), 3) AS prior_rows_12h_per_prior_unit_price_12h,
    prior_rows_12h + prior_unit_price_12h AS prior_rows_12h_plus_prior_unit_price_12h
FROM without_matches
ORDER BY prior_rows_12h_per_prior_unit_price_12h DESC
