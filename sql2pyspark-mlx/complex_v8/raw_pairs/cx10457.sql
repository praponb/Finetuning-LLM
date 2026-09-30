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
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_unit_price_1d,
        sk.labels AS sku_labels
    FROM covered s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        moved_at,
        prior_unit_price_1d,
        sku_labels,
        FIRST_VALUE(prior_unit_price_1d) OVER (ORDER BY prior_unit_price_1d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_unit_price_1d,
        ROW_NUMBER() OVER (ORDER BY prior_unit_price_1d ASC, id) AS seq_no,
        MIN(prior_unit_price_1d) OVER w_sku_id_moved_at_month AS sku_id_moved_at_month_min_prior_unit_price_1d,
        SUM(prior_unit_price_1d) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at)) AS sku_id_moved_at_month_sum_prior_unit_price_1d,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (ORDER BY prior_unit_price_1d ASC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM with_details
    WINDOW w_sku_id_moved_at_month AS (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at))
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels_2,
        sk.sku_code
    FROM sequenced s
    JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost > 108.5
)
-- Subtotals with ROLLUP
SELECT
    seq_no,
    sku_id,
    MAX(moved_at) AS last_moved_at
FROM combined_details_sku
GROUP BY ROLLUP(seq_no, sku_id)
ORDER BY seq_no NULLS LAST, sku_id NULLS LAST
