WITH filtered AS (
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
        LENGTH(TRIM(move_type)) AS move_type_len,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE unit_price <= 367.7
      AND operator IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY move_type_len, id) AS seq_no,
        MAX(move_type_len) OVER (PARTITION BY warehouse_id ORDER BY move_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_move_type_len_so_far,
        NTILE(4) OVER w_warehouse_id_by_move_type_le AS move_type_len_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY move_type_len), 3) AS move_type_len_percent_rank,
        SUM(move_type_len) OVER (PARTITION BY warehouse_id ORDER BY move_type_len, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS move_type_len_sum_4
    FROM filtered
    WINDOW w_warehouse_id_by_move_type_le AS (PARTITION BY warehouse_id ORDER BY move_type_len, id)
)
SELECT *
FROM (
    SELECT move_type_len_quartile, move_type, max_move_type_len_so_far
    FROM with_trends
) src
PIVOT (
    SUM(max_move_type_len_so_far)
    FOR move_type IN ('OUTBOUND', 'ADJUST')
)
ORDER BY move_type_len_quartile
