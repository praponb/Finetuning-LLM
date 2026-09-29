WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE (move_type = 'TRANSFER' OR sku_id <= 18)
),
latest_per_key AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price_adj,
        operator_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY unit_price_adj DESC, id DESC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price_adj,
        operator_ref,
        ROUND(unit_price_adj / NULLIF(SUM(unit_price_adj) OVER (PARTITION BY move_type, operator), 0), 4) AS unit_price_adj_share_of_move_type_operator,
        LAST_VALUE(unit_price_adj) IGNORE NULLS OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_adj,
        LAST_VALUE(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, operator)) AS distinct_sku_id_in_move_type_operator,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type ORDER BY unit_price_adj DESC), 3) AS unit_price_adj_percent_rank,
        SUM(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM latest_per_key
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY operator ORDER BY moved_at, id) AS moved_at_quintile,
        SUM(running_qty) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_qty
    FROM windowed
),
latest_per_key_2 AS (
    -- Keep the latest row per operator
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price_adj,
        operator_ref,
        unit_price_adj_share_of_move_type_operator,
        last_known_unit_price_adj,
        last_qty,
        distinct_sku_id_in_move_type_operator,
        unit_price_adj_percent_rank,
        running_qty,
        moved_at_quintile,
        running_running_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY running_running_qty DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        move_type,
        operator,
        unit_price_adj,
        operator_ref,
        last_known_unit_price_adj,
        last_qty,
        moved_at_quintile,
        running_running_qty,
        SUM(unit_price_adj) OVER w_sku_id_move_type AS sku_id_move_type_sum_unit_price_adj,
        NTH_VALUE(running_qty, 2) OVER (ORDER BY unit_price_adj_percent_rank ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_qty,
        NTILE(5) OVER w_all_by_running_qty AS running_qty_quintile,
        NTH_VALUE(moved_at_quintile, 2) OVER (ORDER BY unit_price_adj_percent_rank ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_quintile
    FROM latest_per_key_2
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type), w_all_by_running_qty AS (ORDER BY running_qty, id)
)
-- Final projection
SELECT
    id,
    move_type,
    operator,
    unit_price_adj,
    operator_ref,
    last_known_unit_price_adj,
    last_qty,
    moved_at_quintile,
    running_running_qty,
    second_running_qty
FROM with_windows
ORDER BY last_qty, running_running_qty
