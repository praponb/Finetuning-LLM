WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty < 300
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price ASC, id) AS unit_price_dense_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, operator), 2) AS unit_price_vs_sku_id_operator_avg
    FROM cleaned_moves
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        unit_price,
        unit_price_dense_rank,
        unit_price_vs_sku_id_operator_avg,
        unit_price_dense_rank - LAG(unit_price_dense_rank) OVER (ORDER BY unit_price_vs_sku_id_operator_avg DESC, id) AS unit_price_dense_rank_change,
        MAX(unit_price_dense_rank) OVER (PARTITION BY operator ORDER BY unit_price_dense_rank DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_dense_rank_max_2,
        NTILE(5) OVER (ORDER BY unit_price_vs_sku_id_operator_avg DESC, id) AS unit_price_vs_sku_id_operator_avg_quintile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_id_move_type, 0), 4) AS unit_price_share_of_warehouse_id_move_type,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, move_type)) AS distinct_operator_in_warehouse_id_move_type,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price_vs_sku_id_operator_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price
    FROM metrics
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
)
SELECT
    unit_price_dense_rank,
    distinct_operator_in_warehouse_id_move_type,
    COALESCE(move_type, 'ALL') AS move_type,
    GROUPING(unit_price_dense_rank) AS is_total_unit_price_dense_rank,
    COUNT(*) FILTER (WHERE operator = 'op_bee') AS n_operator_op_bee
FROM windowed
GROUP BY CUBE(unit_price_dense_rank, distinct_operator_in_warehouse_id_move_type, move_type)
ORDER BY unit_price_dense_rank NULLS LAST, distinct_operator_in_warehouse_id_move_type NULLS LAST, move_type NULLS LAST
