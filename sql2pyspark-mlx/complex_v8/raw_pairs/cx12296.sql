WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE operator <> 'op_dew'
      AND move_type IS NOT NULL
),
combined_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.move_type_group,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(qty) OVER (PARTITION BY sku_is_active, TO_DATE(moved_at) ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_is_active), 2) AS qty_vs_sku_is_active_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty NULLS LAST), 3) AS qty_percent_rank,
        COUNT(*) OVER (PARTITION BY sku_is_active, TO_DATE(moved_at) ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(qty, 2) OVER (PARTITION BY sku_is_active, TO_DATE(moved_at) ORDER BY qty, id) AS next2_qty,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_category, sku_id ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        MIN(sku_unit_cost) OVER (PARTITION BY sku_is_active) AS sku_is_active_min_sku_unit_cost,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        ROUND(AVG(qty) OVER (ORDER BY qty, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_is_active, move_type), 0), 4) AS qty_share_of_sku_is_active_move_type,
        COUNT(*) OVER w_sku_is_active_move_type AS rows_in_sku_is_active_move_type
    FROM combined_details_sku
    WINDOW w_sku_is_active_move_type AS (PARTITION BY sku_is_active, move_type)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(running_count, 0) AS running_count_filled,
        running_count - qty_vs_sku_is_active_avg AS running_count_minus_qty_vs_sku_is_active_avg,
        SUBSTRING(TRIM(move_type_group), 1, 2) AS move_type_group_prefix
    FROM sequenced
    WHERE qty_percent_rank < 0.9
),
rollup_base AS (
    -- Group-level totals
    SELECT
        sku_id,
        MIN(sku_unit_cost) AS min_sku_unit_cost,
        MAX(qty_vs_sku_is_active_avg) AS max_qty_vs_sku_is_active_avg,
        SUM(qty_share_of_sku_is_active_move_type) FILTER (WHERE sku_is_active = FALSE) AS qty_share_of_sku_is_active_move_type_sku_is_acti
    FROM enriched
    GROUP BY sku_id
)
-- Final projection
SELECT
    sku_id,
    max_qty_vs_sku_is_active_avg,
    qty_share_of_sku_is_active_move_type_sku_is_acti,
    COALESCE(min_sku_unit_cost, 0.0) AS min_sku_unit_cost_filled
FROM rollup_base
WHERE max_qty_vs_sku_is_active_avg BETWEEN -450.0 AND 450.0
ORDER BY min_sku_unit_cost_filled, sku_id
LIMIT 10
