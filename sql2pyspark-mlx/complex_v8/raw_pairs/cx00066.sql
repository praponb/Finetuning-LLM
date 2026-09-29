WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-25 07:00:00'
      AND sku_id <= 8
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST) AS moved_at_dense_rank,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price
    FROM valid_moves
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(unit_price) OVER (ORDER BY moved_at NULLS LAST, id) AS prev_unit_price,
        NTILE(10) OVER (ORDER BY moved_at NULLS LAST, id) AS moved_at_decile,
        RANK() OVER (ORDER BY moved_at NULLS LAST) AS moved_at_rank
    FROM with_windows
),
enriched_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.second_unit_price,
        s.moved_at_decile,
        s.moved_at_rank,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code,
        sk.labels AS sku_labels
    FROM windowed s
    JOIN skus sk ON sk.id = s.sku_id
),
operator_stats AS (
    -- Median moved_at_decile per operator
    SELECT operator, PERCENTILE_APPROX(moved_at_decile, 0.5) AS median_moved_at_decile
    FROM enriched_sku
    GROUP BY operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_moved_at_decile,
        ROUND(s.moved_at_decile - g.median_moved_at_decile, 2) AS moved_at_decile_minus_median
    FROM enriched_sku s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.moved_at_decile >= g.median_moved_at_decile
)
-- Final projection
SELECT
    id,
    sku_id,
    operator,
    unit_price,
    sku_weight_kg,
    sku_code,
    ARRAY_CONTAINS(sku_labels, 'fragile') AS has_fragile,
    ARRAY_JOIN(ARRAY_SORT(sku_labels), ',') AS sku_labels_text
FROM outliers
WHERE moved_at_decile <= 8
ORDER BY sku_id, id
