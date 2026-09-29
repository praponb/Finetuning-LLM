WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price,
        CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        greatest_qty_unit_price,
        move_type_group,
        MIN(greatest_qty_unit_price) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_min_greatest_qty_unit_price,
        ROUND(greatest_qty_unit_price - AVG(greatest_qty_unit_price) OVER (PARTITION BY sku_id), 2) AS greatest_qty_unit_price_vs_sku_id_avg,
        RANK() OVER (PARTITION BY operator ORDER BY moved_at DESC, id) AS moved_at_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY operator, sku_id ORDER BY moved_at), 3) AS moved_at_cume_dist,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id), 0), 4) AS unit_price_share_of_sku_id_warehouse_id,
        ROUND(greatest_qty_unit_price / NULLIF(SUM(greatest_qty_unit_price) OVER w_sku_id_warehouse_id, 0), 4) AS greatest_qty_unit_price_share_of_sku_id_warehous,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator ORDER BY moved_at DESC, id), 3) AS moved_at_percent_rank,
        MAX(greatest_qty_unit_price) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_greatest_qty_unit_price_so_far,
        COUNT(*) OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(greatest_qty_unit_price) IGNORE NULLS OVER (PARTITION BY operator, sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_greatest_qty_unit_price
    FROM base
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE moved_at_rank <= 3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        greatest_qty_unit_price,
        move_type_group,
        sku_id_warehouse_id_min_greatest_qty_unit_price,
        greatest_qty_unit_price_vs_sku_id_avg,
        moved_at_rank,
        unit_price_vs_sku_id_avg,
        moved_at_cume_dist,
        unit_price_share_of_sku_id_warehouse_id,
        moved_at_percent_rank,
        max_greatest_qty_unit_price_so_far,
        first_known_greatest_qty_unit_price,
        LAST_VALUE(moved_at_cume_dist) IGNORE NULLS OVER (ORDER BY max_greatest_qty_unit_price_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY max_greatest_qty_unit_price_so_far), 3) AS max_greatest_qty_unit_price_so_far_cume_dist,
        COUNT(*) OVER (PARTITION BY moved_at_rank, operator) AS rows_in_moved_at_rank_operator,
        ROUND(AVG(greatest_qty_unit_price) OVER (ORDER BY sku_id_warehouse_id_min_greatest_qty_unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS greatest_qty_unit_price_avg_6_prior,
        moved_at_rank - LAG(moved_at_rank) OVER (ORDER BY sku_id_warehouse_id_min_greatest_qty_unit_price, id) AS moved_at_rank_change,
        FIRST_VALUE(unit_price_vs_sku_id_avg) OVER (ORDER BY max_greatest_qty_unit_price_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price_vs_sku_id_avg,
        ROUND(moved_at_rank / NULLIF(SUM(moved_at_rank) OVER (PARTITION BY moved_at_rank, operator), 0), 4) AS moved_at_rank_share_of_moved_at_rank_operator
    FROM first_rows
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    greatest_qty_unit_price,
    sku_id_warehouse_id_min_greatest_qty_unit_price,
    greatest_qty_unit_price_vs_sku_id_avg,
    moved_at_rank,
    unit_price_vs_sku_id_avg,
    moved_at_cume_dist,
    unit_price_share_of_sku_id_warehouse_id,
    moved_at_percent_rank,
    max_greatest_qty_unit_price_so_far,
    last_known_moved_at_cume_dist,
    max_greatest_qty_unit_price_so_far_cume_dist,
    rows_in_moved_at_rank_operator,
    greatest_qty_unit_price_avg_6_prior,
    first_unit_price_vs_sku_id_avg,
    SUBSTRING(TRIM(move_type_group), 1, 3) AS move_type_group_prefix
FROM sequenced
