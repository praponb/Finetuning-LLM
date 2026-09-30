WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE (qty >= 98 OR operator = 'op_chan')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_filled,
        sk.sku_code
    FROM cleaned_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(qty) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_max_qty,
        ROUND(AVG(unit_price_filled) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price_filled ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_filled_avg_6_prior,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, sku_id)) AS distinct_warehouse_id_in_operator_sku_id,
        ROUND(CUME_DIST() OVER w_all_by_qty_ties, 3) AS qty_cume_dist,
        SUM(unit_price_filled) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_sum_unit_price_filled,
        RANK() OVER (PARTITION BY operator ORDER BY unit_price_filled NULLS LAST) AS unit_price_filled_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator, sku_id), 2) AS unit_price_vs_operator_sku_id_avg,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price_filled ASC) AS unit_price_filled_dense_rank,
        MAX(unit_price_filled) OVER (PARTITION BY operator ORDER BY unit_price_filled NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_filled_max_7,
        COUNT(*) OVER (PARTITION BY operator, sku_id) AS rows_in_operator_sku_id
    FROM combined_details
    WINDOW w_all_by_qty_ties AS (ORDER BY qty)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        operator,
        unit_price_filled,
        sku_code,
        operator_sku_id_max_qty,
        unit_price_filled_avg_6_prior,
        distinct_warehouse_id_in_operator_sku_id,
        qty_cume_dist,
        operator_sku_id_sum_unit_price_filled,
        COUNT(*) OVER w_operator AS rows_in_operator,
        DENSE_RANK() OVER (PARTITION BY unit_price_filled_dense_rank, distinct_warehouse_id_in_operator_sku_id ORDER BY moved_at DESC, id) AS moved_at_dense_rank,
        ROUND(operator_sku_id_max_qty - AVG(operator_sku_id_max_qty) OVER (PARTITION BY operator), 2) AS operator_sku_id_max_qty_vs_operator_avg,
        DENSE_RANK() OVER (ORDER BY rows_in_operator_sku_id DESC, id) AS rows_in_operator_sku_id_dense_rank
    FROM ranked
    WINDOW w_operator AS (PARTITION BY operator)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        moved_at_dense_rank,
        operator,
        COUNT(*) AS row_count,
        ROUND(SUM(qty_cume_dist) / NULLIF(SUM(rows_in_operator), 0), 3) AS qty_cume_dist_to_rows_in_operator_ratio,
        SUM(CASE WHEN operator = 'op_dew' THEN distinct_warehouse_id_in_operator_sku_id ELSE 0 END) AS distinct_warehouse_id_in_operator_sku_id_operato,
        SUM(CASE WHEN operator IN ('op_eve', 'op_bee') THEN 1 ELSE 0 END) AS n_operator_in_op_eve_op_bee,
        ROUND(SUM(moved_at_dense_rank) / NULLIF(SUM(operator_sku_id_max_qty), 0), 3) AS moved_at_dense_rank_to_operator_sku_id_max_qty_r,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM ranked_2
    GROUP BY moved_at_dense_rank, operator
)
-- Final projection
SELECT
    moved_at_dense_rank,
    operator,
    row_count,
    distinct_warehouse_id_in_operator_sku_id_operato,
    moved_at_dense_rank_to_operator_sku_id_max_qty_r,
    operator_list,
    INITCAP(TRIM(operator)) AS operator_clean,
    ABS(moved_at_dense_rank - 6) AS moved_at_dense_rank_distance
FROM stats
WHERE moved_at_dense_rank_to_operator_sku_id_max_qty_r > 2.0
ORDER BY moved_at_dense_rank, row_count DESC
