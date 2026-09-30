WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
    WHERE sku_id >= 4
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price) OVER (PARTITION BY operator, sku_id ORDER BY unit_price ASC, id) AS next_unit_price,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(unit_price_adj - AVG(unit_price_adj) OVER w_operator, 2) AS unit_price_adj_vs_operator_avg,
        unit_price_adj - LAG(unit_price_adj) OVER w_warehouse_id_by_unit_price_a AS unit_price_adj_change,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        COUNT(*) OVER (ORDER BY unit_price_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER (PARTITION BY warehouse_id, operator) AS rows_in_warehouse_id_operator,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price_adj DESC NULLS FIRST) AS unit_price_adj_dense_rank
    FROM base
    WINDOW w_operator AS (PARTITION BY operator), w_warehouse_id_by_unit_price_a AS (PARTITION BY warehouse_id ORDER BY unit_price_adj DESC NULLS FIRST, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        unit_price,
        move_type_operator_label,
        next_unit_price,
        second_unit_price,
        unit_price_adj_vs_operator_avg,
        rows_in_warehouse_id_operator,
        unit_price_adj_dense_rank,
        ROW_NUMBER() OVER w_all_by_second_unit_price AS seq_no,
        RANK() OVER (ORDER BY second_unit_price DESC, id) AS second_unit_price_rank,
        ROUND(CUME_DIST() OVER w_unit_price_adj_dense_rank_op, 3) AS rows_in_warehouse_id_operator_cume_dist,
        FIRST_VALUE(unit_price_adj_change) OVER (PARTITION BY unit_price_adj_dense_rank, operator ORDER BY rows_in_warehouse_id_operator ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price_adj_change,
        LEAD(unit_price_adj_change, 3, 0) OVER w_all_by_second_unit_price AS next3_unit_price_adj_change,
        ROUND(PERCENT_RANK() OVER w_all_by_second_unit_price, 3) AS second_unit_price_percent_rank
    FROM windowed
    WINDOW w_all_by_second_unit_price AS (ORDER BY second_unit_price DESC, id), w_unit_price_adj_dense_rank_op AS (PARTITION BY unit_price_adj_dense_rank, operator ORDER BY rows_in_warehouse_id_operator ASC)
)
SELECT
    id,
    sku_id,
    unit_price,
    move_type_operator_label,
    second_unit_price,
    rows_in_warehouse_id_operator,
    unit_price_adj_dense_rank,
    seq_no,
    second_unit_price_rank,
    rows_in_warehouse_id_operator_cume_dist,
    next3_unit_price_adj_change,
    second_unit_price_percent_rank
FROM with_trends
ORDER BY id
LIMIT 20
