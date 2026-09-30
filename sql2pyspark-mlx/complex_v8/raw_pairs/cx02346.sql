WITH cleaned_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE is_active = TRUE
      AND category NOT IN ('Food', 'Electronics')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category, is_active ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        COUNT(*) OVER (PARTITION BY category, is_active) AS rows_in_category_is_active,
        ROUND(unit_cost - AVG(unit_cost) OVER w_category_is_active, 2) AS unit_cost_vs_category_is_active_avg,
        ROUND(AVG(unit_cost) OVER (PARTITION BY category, is_active), 2) AS category_is_active_avg_unit_cost,
        LAST_VALUE(unit_cost) OVER (PARTITION BY category, is_active ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost,
        DENSE_RANK() OVER w_category_is_active_by_unit_c_3 AS unit_cost_dense_rank
    FROM cleaned_skus
    WINDOW w_category_is_active AS (PARTITION BY category, is_active), w_category_is_active_by_unit_c_3 AS (PARTITION BY category, is_active ORDER BY unit_cost, id)
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.category,
        s.unit_cost,
        s.labels,
        s.is_active,
        s.rows_in_category_is_active,
        s.category_is_active_avg_unit_cost,
        s.last_unit_cost,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price
    FROM with_windows s
    JOIN stock_moves st ON st.sku_id = s.id
)
SELECT id, move_id, field, reading
FROM (
    SELECT id, move_id, rows_in_category_is_active, move_qty
    FROM exploded_detail_moves
) src
UNPIVOT (
    reading FOR field IN (rows_in_category_is_active, move_qty)
) unp
