WITH cleaned_moves AS (
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
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        LENGTH(TRIM(operator)) AS operator_len,
        CASE WHEN operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE unit_price > 119.5
      AND qty < 393
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        move_type_or_unknown,
        operator_len,
        qty - operator_len AS qty_minus_operator_len
    FROM cleaned_moves
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        move_type_or_unknown,
        qty_minus_operator_len,
        FLOOR(operator_len / 2) * 2 AS operator_len_bucket,
        LEAST(qty_minus_operator_len, operator_len) AS least_qty_minus_operator_len_operator_len,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM enriched
),
latest AS (
    -- Keep the earliest row per move_type_or_unknown
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        move_type_or_unknown,
        qty_minus_operator_len,
        operator_len_bucket,
        least_qty_minus_operator_len_operator_len,
        qty_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type_or_unknown ORDER BY qty_bucket, id) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
)
SELECT *
FROM (
    SELECT operator_len_bucket, move_type, move_type_or_unknown, qty
    FROM latest
) src
PIVOT (
    MAX(qty) AS max_v, COUNT(qty) AS n
    FOR move_type_or_unknown IN ('INBOUND', 'UNKNOWN')
)
ORDER BY operator_len_bucket, move_type
