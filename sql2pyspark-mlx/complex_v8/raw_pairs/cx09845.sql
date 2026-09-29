WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        CASE WHEN unit_price >= 372.6 THEN 'A' WHEN unit_price >= 215.4 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE move_type NOT IN ('OUTBOUND', 'ADJUST')
      AND warehouse_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_or_unknown,
        MAX(qty) OVER w_warehouse_id AS warehouse_id_max_qty,
        LAST_VALUE(unit_price) OVER (PARTITION BY move_type ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_move_type_unit_price_tier, 0), 4) AS unit_price_share_of_move_type_unit_price_tier,
        DENSE_RANK() OVER (PARTITION BY operator, move_type ORDER BY qty DESC, id) AS qty_dense_rank,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST) AS unit_price_dense_rank,
        COUNT(*) OVER w_move_type_unit_price_tier AS rows_in_move_type_unit_price_tier,
        SUM(qty) OVER (PARTITION BY move_type, warehouse_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        SUM(qty) OVER w_move_type_unit_price_tier AS move_type_unit_price_tier_sum_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST), 3) AS unit_price_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST, id) AS unit_price_seq,
        SUM(qty) OVER (PARTITION BY operator ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_2,
        unit_price - LAG(unit_price) OVER w_operator_by_unit_price AS unit_price_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type ORDER BY qty, id), 3) AS qty_percent_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type_unit_price_tier) AS distinct_warehouse_id_in_move_type_unit_price_ti,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, move_type ORDER BY qty DESC), 3) AS qty_percent_rank_2
    FROM recent_moves
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_move_type_unit_price_tier AS (PARTITION BY move_type, unit_price_tier), w_operator_by_unit_price AS (PARTITION BY operator ORDER BY unit_price NULLS LAST, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_or_unknown,
        warehouse_id_max_qty,
        last_unit_price,
        unit_price_share_of_move_type_unit_price_tier,
        unit_price_dense_rank,
        rows_in_move_type_unit_price_tier,
        running_qty,
        move_type_unit_price_tier_sum_qty,
        unit_price_percent_rank,
        unit_price_seq,
        running_qty_2,
        unit_price_change,
        qty_percent_rank,
        distinct_warehouse_id_in_move_type_unit_price_ti,
        qty_percent_rank_2
    FROM with_trends
    WHERE qty_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    operator,
    unit_price,
    move_type_or_unknown,
    last_unit_price,
    unit_price_share_of_move_type_unit_price_tier,
    unit_price_dense_rank,
    running_qty,
    move_type_unit_price_tier_sum_qty,
    unit_price_percent_rank,
    unit_price_seq,
    running_qty_2,
    unit_price_change,
    qty_percent_rank,
    distinct_warehouse_id_in_move_type_unit_price_ti,
    qty_percent_rank_2,
    LOWER(REGEXP_REPLACE(TRIM(move_type_or_unknown), ' ', '_')) AS move_type_or_unknown_slug
FROM first_rows
ORDER BY running_qty_2 NULLS LAST, qty_percent_rank DESC NULLS FIRST
