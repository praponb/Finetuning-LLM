WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 08:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY warehouse_id ORDER BY unit_price_plus_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(unit_price_plus_qty, 1, 0) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price_plus_qty ASC, id) AS prev_unit_price_plus_qty,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price_plus_qty) AS unit_price_plus_qty_rank,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id, warehouse_id)) AS distinct_operator_in_sku_id_warehouse_id,
        ROUND(AVG(unit_price_plus_qty) OVER w_sku_id, 2) AS sku_id_avg_unit_price_plus_qty,
        ROUND(unit_price_plus_qty / NULLIF(SUM(unit_price_plus_qty) OVER (PARTITION BY sku_id, warehouse_id), 0), 4) AS unit_price_plus_qty_share_of_sku_id_warehouse_id,
        SUM(qty) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_sum_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id, warehouse_id), 2) AS qty_vs_sku_id_warehouse_id_avg,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY qty DESC NULLS FIRST, id) AS seq_no,
        LEAD(qty, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price_plus_qty ASC, id) AS next2_qty,
        NTILE(5) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price_plus_qty ASC, id) AS unit_price_plus_qty_quintile,
        LAG(unit_price_plus_qty) OVER w_warehouse_id_by_unit_price_p_3 AS prev_unit_price_plus_qty_2
    FROM recent
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_warehouse_id_by_unit_price_p_3 AS (PARTITION BY warehouse_id ORDER BY unit_price_plus_qty, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        CASE WHEN seq_no >= 8 THEN 'A' WHEN seq_no >= 4 THEN 'B' ELSE 'C' END AS seq_no_tier,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown_2
    FROM windowed
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(seq_no) OVER (PARTITION BY move_type) AS move_type_max_seq_no,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type)) AS distinct_sku_id_in_move_type,
        MAX(rows_in_sku_id) OVER (PARTITION BY operator ORDER BY qty_vs_sku_id_warehouse_id_avg DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rows_in_sku_id_max_3_prior,
        SUM(prev_unit_price_plus_qty) OVER (PARTITION BY operator ORDER BY qty_vs_sku_id_warehouse_id_avg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_unit_price_plus_qty
    FROM annotated
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        operator_or_unknown_2,
        move_type,
        PERCENTILE_APPROX(move_type_max_seq_no, 0.5) AS median_move_type_max_seq_no,
        COUNT(*) AS row_count,
        SUM(CASE WHEN move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND') THEN move_type_max_seq_no ELSE 0 END) AS move_type_max_seq_no_move_type_in_adjust_transfe,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids
    FROM ranked
    GROUP BY operator_or_unknown_2, move_type
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(distinct_sku_ids) OVER w_operator_or_unknown_2_move_t AS prev_distinct_sku_ids,
        LAST_VALUE(median_move_type_max_seq_no) OVER (PARTITION BY operator_or_unknown_2, move_type ORDER BY median_move_type_max_seq_no DESC NULLS FIRST, operator_or_unknown_2, move_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_median_move_type_max_seq_no,
        ROUND(AVG(row_count) OVER (PARTITION BY row_count, distinct_sku_ids), 2) AS row_count_distinct_sku_ids_avg_row_count
    FROM stats
    WINDOW w_operator_or_unknown_2_move_t AS (PARTITION BY operator_or_unknown_2, move_type ORDER BY median_move_type_max_seq_no DESC NULLS FIRST, operator_or_unknown_2, move_type)
)
-- Final projection
SELECT
    operator_or_unknown_2,
    move_type,
    row_count,
    move_type_max_seq_no_move_type_in_adjust_transfe,
    distinct_sku_ids,
    row_count_distinct_sku_ids_avg_row_count,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
FROM ranked_2
ORDER BY row_count
