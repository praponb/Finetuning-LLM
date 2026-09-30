WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        unit_price + qty AS unit_price_plus_qty,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE qty < 355
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id_operator_or_unknown) AS distinct_warehouse_id_in_sku_id_operator_or_unkn,
        DENSE_RANK() OVER (ORDER BY unit_price_adj) AS unit_price_adj_dense_rank,
        SUM(unit_price) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROUND(PERCENT_RANK() OVER w_warehouse_id_by_moved_at, 3) AS moved_at_percent_rank
    FROM base
    WINDOW w_sku_id_operator_or_unknown AS (PARTITION BY sku_id, operator_or_unknown), w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter,
        CASE WHEN operator IN ('op_eve', 'op_chan') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM windowed
    WHERE running_unit_price < 4250.1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at, operator_or_unknown, distinct_warehouse_id_in_sku_id_operator_or_unkn
    FROM enriched
) src
PIVOT (
    MIN(distinct_warehouse_id_in_sku_id_operator_or_unkn)
    FOR operator_or_unknown IN ('op_chan', 'UNKNOWN', 'op_ann', 'op_eve')
)
