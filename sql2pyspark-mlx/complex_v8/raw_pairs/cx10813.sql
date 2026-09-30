WITH matched AS (
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
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        moved_at,
        prior_unit_price_3d,
        COUNT(*) OVER (PARTITION BY prior_rows_3d, sku_id) AS rows_in_prior_rows_3d_sku_id,
        NTILE(3) OVER w_sku_id_by_moved_at AS moved_at_tercile,
        ROUND(AVG(prior_unit_price_3d) OVER (PARTITION BY prior_rows_3d ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS prior_unit_price_3d_avg_3
    FROM matched
    WINDOW w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at DESC, id)
),
combined_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.rows_in_prior_rows_3d_sku_id,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM with_windows s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
qualified AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM combined_details_sku s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.is_active = FALSE
        )
)
-- Final projection
SELECT
    id,
    sku_id,
    sku_labels,
    sku_code
FROM qualified
ORDER BY sku_id DESC, sku_code DESC
