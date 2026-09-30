WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        YEAR(moved_at) AS moved_at_year,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE (operator IS NOT NULL OR qty < 387)
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_year,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.move_type
    FROM cleaned_moves s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.moved_at_year,
        s.move_id,
        s.move_qty,
        s.move_unit_price,
        s.move_type,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity
    FROM line_level s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        unit_price,
        move_id,
        move_unit_price,
        move_type,
        warehouse_capacity,
        ROUND(CUME_DIST() OVER w_move_id_operator_by_moved_at, 3) AS moved_at_year_cume_dist,
        move_qty - LAG(move_qty) OVER (PARTITION BY move_id ORDER BY move_unit_price DESC, id, move_id) AS move_qty_change,
        MAX(unit_price) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price
    FROM combined_details
    WINDOW w_move_id_operator_by_moved_at AS (PARTITION BY move_id, operator ORDER BY moved_at_year DESC, id, move_id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(move_unit_price / NULLIF(SUM(move_unit_price) OVER w_sku_id_operator, 0), 4) AS move_unit_price_share_of_sku_id_operator,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY unit_price ASC) AS unit_price_dense_rank,
        COUNT(*) OVER (PARTITION BY sku_id, operator) AS rows_in_sku_id_operator,
        ROUND(AVG(moved_at_year_cume_dist) OVER (PARTITION BY sku_id ORDER BY sku_id_max_unit_price DESC NULLS FIRST, id, move_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS moved_at_year_cume_dist_avg_3_prior
    FROM with_trends
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE sku_id_max_unit_price > (SELECT AVG(sku_id_max_unit_price) FROM ranked)
)
SELECT
    id,
    sku_id,
    operator,
    move_id,
    move_unit_price,
    move_type,
    warehouse_capacity,
    moved_at_year_cume_dist,
    move_qty_change,
    unit_price_dense_rank,
    rows_in_sku_id_operator,
    moved_at_year_cume_dist_avg_3_prior,
    CASE WHEN rows_in_sku_id_operator >= 13 THEN 'large' WHEN rows_in_sku_id_operator >= 8 THEN 'medium' ELSE 'small' END AS rows_in_sku_id_operator_tier,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM screened
WHERE move_unit_price < 399.6
ORDER BY move_qty_change DESC, operator
