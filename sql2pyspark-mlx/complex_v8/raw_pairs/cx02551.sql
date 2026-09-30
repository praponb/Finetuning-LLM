WITH prepared_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        FLOOR(unit_cost / 50) * 50 AS unit_cost_bucket
    FROM skus
    WHERE is_active = TRUE
      AND unit_cost >= 60.9
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        unit_cost,
        unit_cost_bucket,
        MIN(unit_cost_bucket) OVER (PARTITION BY category) AS category_min_unit_cost_bucket,
        ROUND(CUME_DIST() OVER (PARTITION BY category ORDER BY unit_cost_bucket), 3) AS unit_cost_bucket_cume_dist,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category
    FROM prepared_skus
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.unit_cost,
        s.category_min_unit_cost_bucket,
        s.unit_cost_bucket_cume_dist,
        s.rows_in_category,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM with_windows s
    JOIN stock_moves st ON st.sku_id = s.id AND HOUR(st.moved_at) BETWEEN 9 AND 12
)
-- Final projection
SELECT
    id,
    unit_cost,
    category_min_unit_cost_bucket,
    unit_cost_bucket_cume_dist,
    rows_in_category,
    move_id,
    move_qty,
    FLOOR(unit_cost / 50) * 50 AS unit_cost_bucket,
    rows_in_category + move_qty AS rows_in_category_plus_move_qty
FROM with_children
ORDER BY unit_cost, id DESC
