WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.qty) AS prior_qty_7d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
latest AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        sku_id,
        moved_at,
        prior_rows_7d,
        prior_qty_7d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY prior_rows_7d DESC, id DESC) AS rn
        FROM covered
    ) ranked
    WHERE rn = 1
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_7d,
        s.prior_qty_7d,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels
    FROM latest s
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(prior_qty_7d) OVER (ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_qty_7d,
        ROW_NUMBER() OVER (ORDER BY sku_unit_cost NULLS LAST, id) AS rn,
        MIN(sku_weight_kg) OVER (ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_sku_weight_kg_so_far,
        COUNT(*) OVER (PARTITION BY prior_rows_7d) AS rows_in_prior_rows_7d,
        LAG(sku_weight_kg) OVER (ORDER BY sku_unit_cost NULLS LAST, id) AS prev_sku_weight_kg
    FROM combined_details_sku
    WINDOW w_prior_rows_7d AS (PARTITION BY prior_rows_7d)
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_7d,
    prior_qty_7d,
    sku_weight_kg,
    sku_unit_cost,
    sku_labels,
    first_prior_qty_7d,
    min_sku_weight_kg_so_far,
    rows_in_prior_rows_7d,
    prev_sku_weight_kg,
    ARRAY_CONTAINS(sku_labels, 'hazmat') AS has_hazmat
FROM windowed
ORDER BY prev_sku_weight_kg, first_prior_qty_7d
