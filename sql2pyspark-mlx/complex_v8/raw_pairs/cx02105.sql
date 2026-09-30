WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CASE WHEN qty >= 394 THEN 'large' WHEN qty >= 189 THEN 'medium' ELSE 'small' END AS qty_tier,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE (warehouse_id IS NOT NULL OR operator NOT IN ('op_ann', 'op_eve'))
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        qty_tier,
        move_type_or_unknown,
        LAST_VALUE(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_qty_tier, 0), 4) AS unit_price_share_of_sku_id_qty_tier,
        COUNT(*) OVER (PARTITION BY sku_id, qty_tier) AS rows_in_sku_id_qty_tier,
        SUM(unit_price) OVER (PARTITION BY qty_tier ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY move_type_or_unknown ORDER BY moved_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY unit_price, id) AS unit_price_seq,
        ROUND(CUME_DIST() OVER w_sku_id_by_unit_price_ties, 3) AS unit_price_cume_dist,
        unit_price - LAG(unit_price) OVER w_qty_tier_by_unit_price AS unit_price_change
    FROM prepared
    WINDOW w_sku_id_qty_tier AS (PARTITION BY sku_id, qty_tier), w_sku_id_by_unit_price_ties AS (PARTITION BY sku_id ORDER BY unit_price), w_qty_tier_by_unit_price AS (PARTITION BY qty_tier ORDER BY unit_price ASC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        qty_tier,
        move_type_or_unknown,
        last_unit_price,
        unit_price_share_of_sku_id_qty_tier,
        rows_in_sku_id_qty_tier,
        running_unit_price,
        qty_avg_1_prior,
        unit_price_cume_dist,
        unit_price_change
    FROM with_trends
    WHERE unit_price_seq = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY qty_tier, move_type_or_unknown)) AS distinct_sku_id_in_qty_tier_move_type_or_unknown,
        ROUND(running_unit_price - AVG(running_unit_price) OVER w_qty_tier_move_type_or_unknow, 2) AS running_unit_price_vs_qty_tier_move_type_or_unkn,
        LAST_VALUE(qty) OVER (PARTITION BY qty_tier ORDER BY qty_avg_1_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY qty_tier ORDER BY qty_avg_1_prior ASC, id), 3) AS qty_avg_1_prior_percent_rank,
        ROUND(qty / NULLIF(SUM(qty) OVER w_qty_tier_move_type_or_unknow, 0), 4) AS qty_share_of_qty_tier_move_type_or_unknown
    FROM top_rows
    WINDOW w_qty_tier_move_type_or_unknow AS (PARTITION BY qty_tier, move_type_or_unknown)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    moved_at,
    unit_price,
    qty_tier,
    last_unit_price,
    unit_price_share_of_sku_id_qty_tier,
    rows_in_sku_id_qty_tier,
    running_unit_price,
    qty_avg_1_prior,
    unit_price_cume_dist,
    unit_price_change,
    last_qty,
    qty_avg_1_prior_percent_rank,
    qty_share_of_qty_tier_move_type_or_unknown
FROM ranked
WHERE warehouse_id IS NOT NULL
ORDER BY moved_at DESC, id DESC
