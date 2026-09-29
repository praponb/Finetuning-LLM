WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE qty >= 116
      AND unit_price <= 298.9
),
per_group AS (
    -- Group-level totals
    SELECT
        move_type,
        SUM(CASE WHEN operator IS NOT NULL THEN 1 ELSE 0 END) AS n_operator_is_not_null,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ROUND(100 * COUNT(CASE WHEN move_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_move_type_is_not_null,
        COUNT(*) AS cnt,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
    FROM cleaned
    GROUP BY move_type
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(move_type, '#', CAST(distinct_warehouse_ids AS STRING)) AS move_type_ref,
        cnt + n_operator_is_not_null AS cnt_plus_n_operator_is_not_null
    FROM per_group
    WHERE cnt < 11
),
exploded AS (
    -- One row per move_type_list_item
    SELECT
        s.move_type,
        s.n_operator_is_not_null,
        s.pct_move_type_is_not_null,
        s.cnt,
        s.distinct_warehouse_ids,
        s.move_type_ref,
        s.cnt_plus_n_operator_is_not_null,
        t.move_type_list_item
    FROM labeled s
    LATERAL VIEW OUTER EXPLODE(s.move_type_list) t AS move_type_list_item
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM exploded
    WHERE n_operator_is_not_null < (SELECT MAX(n_operator_is_not_null) FROM exploded)
)
SELECT *
FROM (
    SELECT cnt, move_type, n_operator_is_not_null
    FROM without_matches
) src
PIVOT (
    AVG(n_operator_is_not_null) AS avg_v, MAX(n_operator_is_not_null) AS max_v
    FOR move_type IN ('OUTBOUND', 'INBOUND', 'TRANSFER')
)
ORDER BY cnt
