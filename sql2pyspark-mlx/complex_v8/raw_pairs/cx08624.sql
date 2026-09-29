WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
tagged AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
    FROM base
    WHERE sku_id >= 9
),
unique_rows AS (
    -- Keep the latest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY moved_at DESC, id DESC) AS rn
        FROM tagged
    ) ranked
    WHERE rn = 1
),
stats AS (
    -- Group-level totals
    SELECT
        operator,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        sku_id,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM unique_rows
    WHERE sku_id IS NOT NULL
    GROUP BY operator, DATE_TRUNC('MONTH', moved_at), sku_id
    HAVING COUNT(*) >= 2
),
per_label AS (
    -- One row per operator_list_item
    SELECT
        s.operator,
        s.moved_at_month,
        s.sku_id,
        s.cnt,
        s.distinct_warehouse_ids,
        s.avg_unit_price,
        t.operator_list_item
    FROM stats s
    LATERAL VIEW EXPLODE(s.operator_list) t AS operator_list_item
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.operator,
        s.moved_at_month,
        s.sku_id,
        s.cnt,
        s.distinct_warehouse_ids,
        s.operator_list_item,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.warehouse_id AS move_warehouse_id
    FROM per_label s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    operator,
    sku_id,
    cnt,
    distinct_warehouse_ids,
    operator_list_item,
    move_id,
    move_moved_at,
    move_warehouse_id
FROM exploded_detail_moves
ORDER BY move_id DESC, sku_id DESC
