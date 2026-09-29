WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.qty) AS prior_qty_12h
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.sku_id, a.moved_at
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        prior_qty_12h,
        LEAST(prior_qty_12h, prior_rows_12h) AS least_prior_qty_12h_prior_rows_12h,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM covered
    WHERE prior_qty_12h BETWEEN 250 AND 1875
),
latest AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        sku_id,
        prior_qty_12h,
        least_prior_qty_12h_prior_rows_12h,
        moved_at_week
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY prior_qty_12h DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
with_parent_sku AS (
    -- Join skus
    SELECT s.*, sk.sku_code
    FROM latest s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    prior_qty_12h,
    least_prior_qty_12h_prior_rows_12h,
    moved_at_week,
    sku_code,
    DATE_TRUNC('MONTH', moved_at_week) AS moved_at_week_month
FROM with_parent_sku
WHERE prior_qty_12h <= 1375
