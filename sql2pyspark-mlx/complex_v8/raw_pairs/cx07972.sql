WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_group,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        sk.is_active AS sku_is_active
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_group,
        sku_code,
        sku_is_active,
        LAG(sku_unit_cost, 3) OVER (ORDER BY unit_price DESC, id) AS prev3_sku_unit_cost,
        ROW_NUMBER() OVER w_warehouse_id_by_moved_at AS moved_at_seq,
        ROUND(AVG(qty) OVER w_warehouse_id, 2) AS warehouse_id_avg_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY sku_unit_cost DESC NULLS FIRST, id), 3) AS sku_unit_cost_cume_dist,
        NTILE(4) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC NULLS FIRST, id) AS moved_at_quartile,
        LAST_VALUE(qty) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER w_moved_at_to_date_move_type_g, 0), 4) AS sku_unit_cost_share_of_moved_at_to_date_move_typ,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY TO_DATE(moved_at), move_type_group), 2) AS moved_at_to_date_move_type_group_avg_sku_unit_co,
        DENSE_RANK() OVER (ORDER BY unit_price DESC, id) AS unit_price_dense_rank,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY TO_DATE(moved_at), move_type_group), 2) AS sku_unit_cost_vs_moved_at_to_date_move_type_grou,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_2_prior,
        RANK() OVER (ORDER BY unit_price DESC) AS unit_price_rank,
        FIRST_VALUE(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        COUNT(*) OVER (PARTITION BY TO_DATE(moved_at), move_type_group) AS rows_in_moved_at_to_date_move_type_group,
        qty - LAG(qty) OVER w_warehouse_id_by_moved_at AS qty_change,
        unit_price - LAG(unit_price) OVER (ORDER BY unit_price DESC, id) AS unit_price_change
    FROM enriched
    WINDOW w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at DESC NULLS FIRST, id), w_warehouse_id AS (PARTITION BY warehouse_id), w_moved_at_to_date_move_type_g AS (PARTITION BY TO_DATE(moved_at), move_type_group)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price,
        move_type_group,
        sku_is_active,
        prev3_sku_unit_cost,
        moved_at_seq,
        moved_at_quartile,
        sku_unit_cost_share_of_moved_at_to_date_move_typ,
        moved_at_to_date_move_type_group_avg_sku_unit_co,
        unit_price_dense_rank,
        sku_unit_cost_vs_moved_at_to_date_move_type_grou,
        unit_price_rank,
        rows_in_moved_at_to_date_move_type_group,
        unit_price_change,
        COUNT(*) OVER (PARTITION BY unit_price_dense_rank) AS rows_in_unit_price_dense_rank,
        FIRST_VALUE(unit_price_dense_rank) IGNORE NULLS OVER (ORDER BY first_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_dense_rank,
        FIRST_VALUE(unit_price_change) OVER (PARTITION BY sku_id ORDER BY moved_at_quartile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price_change,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY unit_price_dense_rank)) AS distinct_sku_id_in_unit_price_dense_rank
    FROM with_trends
)
SELECT
    id,
    warehouse_id,
    moved_at,
    unit_price,
    sku_is_active,
    moved_at_quartile,
    moved_at_to_date_move_type_group_avg_sku_unit_co,
    rows_in_moved_at_to_date_move_type_group,
    unit_price_change,
    rows_in_unit_price_dense_rank,
    first_known_unit_price_dense_rank,
    first_unit_price_change,
    distinct_sku_id_in_unit_price_dense_rank,
    LOWER(TRIM(move_type_group)) AS move_type_group_clean,
    DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
FROM windowed
WHERE first_unit_price_change < 150.0
ORDER BY moved_at, moved_at_hour DESC, id
LIMIT 20
