WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(move_type_len) OVER (PARTITION BY sku_id ORDER BY move_type_len DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS move_type_len_avg_3_prior,
        ROUND(move_type_len / NULLIF(SUM(move_type_len) OVER (PARTITION BY sku_id), 0), 4) AS move_type_len_share_of_sku_id,
        ROUND(AVG(move_type_len) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_move_type_len
    FROM prepared_moves
),
latest_per_key AS (
    -- Keep the earliest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        move_type_len,
        move_type_len_avg_3_prior,
        move_type_len_share_of_sku_id,
        sku_id_avg_move_type_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY move_type_len_share_of_sku_id, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the earliest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        move_type_len,
        move_type_len_avg_3_prior,
        move_type_len_share_of_sku_id,
        sku_id_avg_move_type_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY moved_at, id) AS rn
        FROM latest_per_key
    ) ranked
    WHERE rn = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        operator,
        PERCENTILE_APPROX(move_type_len_share_of_sku_id, 0.5) AS median_move_type_len_share_of_sku_id,
        ROUND(AVG(sku_id_avg_move_type_len), 2) AS avg_sku_id_avg_move_type_len,
        SUM(CASE WHEN move_type IN ('INBOUND', 'ADJUST') THEN 1 ELSE 0 END) AS n_move_type_in_inbound_adjust
    FROM unique_rows
    GROUP BY 1
    HAVING AVG(move_type_len_share_of_sku_id) > 0.2
)
-- Subtotals with GROUPING SETS
SELECT
    n_move_type_in_inbound_adjust,
    operator,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
    SUM(CASE WHEN operator IN ('op_eve', 'op_bee', 'op_chan') THEN 1 ELSE 0 END) AS n_operator_in_op_eve_op_bee_op
FROM grouped
GROUP BY GROUPING SETS ((n_move_type_in_inbound_adjust, operator), (n_move_type_in_inbound_adjust), ())
ORDER BY n_move_type_in_inbound_adjust NULLS LAST, operator NULLS LAST
