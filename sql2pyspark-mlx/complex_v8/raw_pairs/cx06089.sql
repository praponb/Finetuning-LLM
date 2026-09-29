WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_dew'
),
enriched AS (
    -- Label and score rows
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM scoped
),
deduped AS (
    -- Keep the earliest row per move_type, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Keep the latest row per warehouse_id, operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, operator ORDER BY unit_price DESC, id DESC) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        operator_slug,
        RANK() OVER w_operator_sku_id_by_moved_at AS moved_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY qty DESC, id), 3) AS qty_cume_dist,
        SUM(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_qty,
        LEAD(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY moved_at DESC, id) AS next_unit_price
    FROM latest
    WINDOW w_operator_sku_id_by_moved_at AS (PARTITION BY operator, sku_id ORDER BY moved_at DESC)
),
unique_rows AS (
    -- Keep the latest row per operator
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        operator_slug,
        moved_at_rank,
        qty_cume_dist,
        warehouse_id_sum_qty,
        next_unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY warehouse_id_sum_qty DESC, id DESC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    warehouse_id,
    move_type,
    operator_slug,
    moved_at_rank,
    qty_cume_dist,
    next_unit_price
FROM unique_rows
ORDER BY id, moved_at_rank DESC
