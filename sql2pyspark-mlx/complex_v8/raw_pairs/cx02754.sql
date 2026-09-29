WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND warehouse_id <= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(qty) OVER (PARTITION BY operator ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_3_prior,
        MAX(qty) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_max_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator), 2) AS qty_vs_operator_avg,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price), 3) AS unit_price_cume_dist,
        NTILE(10) OVER w_all_by_unit_price AS unit_price_decile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator), 0), 4) AS unit_price_share_of_operator,
        qty - LAG(qty) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS qty_change,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator)) AS distinct_sku_id_in_operator
    FROM cleaned
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price DESC NULLS FIRST, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(operator)) AS operator_clean,
        CONCAT(operator, '#', CAST(qty_change AS STRING)) AS operator_ref,
        ABS(unit_price - 255.6) AS unit_price_distance
    FROM ranked
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty_avg_3_prior / NULLIF(SUM(qty_avg_3_prior) OVER w_operator, 0), 4) AS qty_avg_3_prior_share_of_operator,
        MAX(unit_price_cume_dist) OVER w_operator AS operator_max_unit_price_cume_dist,
        ROUND(unit_price_decile / NULLIF(SUM(unit_price_decile) OVER (PARTITION BY operator), 0), 4) AS unit_price_decile_share_of_operator,
        FIRST_VALUE(unit_price_cume_dist) IGNORE NULLS OVER (PARTITION BY unit_price_decile, operator ORDER BY unit_price_cume_dist NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_cume_dist,
        DENSE_RANK() OVER w_unit_price_decile_operator_b_2 AS unit_price_cume_dist_dense_rank,
        ROUND(sku_id_warehouse_id_max_qty - AVG(sku_id_warehouse_id_max_qty) OVER (PARTITION BY operator), 2) AS sku_id_warehouse_id_max_qty_vs_operator_avg,
        ROUND(qty_change - AVG(qty_change) OVER w_operator, 2) AS qty_change_vs_operator_avg
    FROM derived
    WINDOW w_operator AS (PARTITION BY operator), w_unit_price_decile_operator_b_2 AS (PARTITION BY unit_price_decile, operator ORDER BY unit_price_cume_dist NULLS LAST)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        operator,
        warehouse_id,
        distinct_sku_id_in_operator,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(*) FILTER (WHERE operator NOT IN ('op_dew', 'op_chan')) AS n_operator_not_in_op_dew_op_ch,
        COUNT(*) FILTER (WHERE operator IN ('op_eve', 'op_chan', 'op_bee')) AS n_operator_in_op_eve_op_chan_o
    FROM sequenced
    GROUP BY 1, 2, 3
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(distinct_sku_id_in_operator - AVG(distinct_sku_id_in_operator) OVER w_n_operator_in_op_eve_op_chan, 2) AS distinct_sku_id_in_operator_vs_n_operator_in_op_,
        NTH_VALUE(distinct_sku_id_in_operator, 2) OVER (ORDER BY n_operator_not_in_op_dew_op_ch DESC, operator, warehouse_id, distinct_sku_id_in_operator ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_sku_id_in_operator,
        ROUND(AVG(distinct_sku_id_in_operator) OVER (ORDER BY n_operator_not_in_op_dew_op_ch DESC, operator, warehouse_id, distinct_sku_id_in_operator ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS distinct_sku_id_in_operator_avg_7
    FROM rollup_base
    WINDOW w_n_operator_in_op_eve_op_chan AS (PARTITION BY n_operator_in_op_eve_op_chan_o)
)
-- Final projection
SELECT
    operator,
    distinct_sku_id_in_operator,
    operator_list,
    n_operator_not_in_op_dew_op_ch,
    n_operator_in_op_eve_op_chan_o,
    distinct_sku_id_in_operator_vs_n_operator_in_op_,
    distinct_sku_id_in_operator_avg_7
FROM sequenced_2
ORDER BY n_operator_in_op_eve_op_chan_o NULLS LAST, n_operator_not_in_op_dew_op_ch DESC NULLS FIRST
