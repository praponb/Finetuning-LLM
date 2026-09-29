WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        QUARTER(moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE (move_type IS NOT NULL OR sku_id IS NOT NULL)
),
unique_rows AS (
    -- Keep the latest row per warehouse_id, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        moved_at_label,
        moved_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at_quarter DESC, id DESC) AS recency_rank
        FROM base
    ) ranked
    WHERE recency_rank = 1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        moved_at_quarter,
        operator,
        PERCENTILE_APPROX(qty, 0.5) AS median_qty,
        ROUND(STDDEV(qty), 2) AS stddev_qty,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM unique_rows
    GROUP BY 1, 2, 3
    HAVING MAX(unit_price) > 60.4
),
detail_rows AS (
    -- One row per move
    SELECT
        s.warehouse_id,
        s.moved_at_quarter,
        s.operator,
        s.stddev_qty,
        s.operator_list,
        st.id AS move_id,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id
    FROM per_group s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.operator IN ('op_dew', 'op_eve', 'op_bee')
)
SELECT
    warehouse_id,
    moved_at_quarter,
    operator,
    stddev_qty,
    operator_list,
    move_operator,
    move_sku_id,
    CONCAT(move_operator, '#', CAST(move_sku_id AS STRING)) AS move_operator_ref,
    COALESCE(stddev_qty, 0.0) AS stddev_qty_filled
FROM detail_rows
ORDER BY operator, warehouse_id
