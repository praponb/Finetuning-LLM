WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_prefix,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator)) AS distinct_sku_id_in_operator,
        ROUND(PERCENT_RANK() OVER w_sku_id_operator_by_qty, 3) AS qty_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_plus_2h) ORDER BY unit_price DESC, id) AS unit_price_seq,
        NTH_VALUE(qty, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_plus_2h) ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(qty - AVG(qty) OVER w_operator, 2) AS qty_vs_operator_avg,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator
    FROM valid_moves
    WINDOW w_sku_id_operator_by_qty AS (PARTITION BY sku_id, operator ORDER BY qty ASC NULLS LAST, id), w_operator AS (PARTITION BY operator)
),
latest AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_prefix,
        distinct_sku_id_in_operator,
        qty_percent_rank,
        unit_price_seq,
        second_qty,
        qty_vs_operator_avg,
        rows_in_operator
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    sku_id,
    qty,
    operator,
    unit_price,
    move_type_prefix,
    distinct_sku_id_in_operator,
    qty_percent_rank,
    unit_price_seq,
    qty_vs_operator_avg,
    ABS(unit_price_seq - 7) AS unit_price_seq_distance
FROM latest
ORDER BY unit_price_seq DESC, id
LIMIT 5
