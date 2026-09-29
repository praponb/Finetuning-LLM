WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        UPPER(TRIM(operator)) AS operator_clean,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price ASC, id) AS unit_price_decile,
        MAX(unit_price) OVER (PARTITION BY move_type) AS move_type_max_unit_price,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty ASC, id) AS seq_no
    FROM filtered_moves
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(unit_price) OVER (PARTITION BY operator) AS operator_min_unit_price,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator)) AS distinct_move_type_in_operator,
        FIRST_VALUE(unit_price) OVER (PARTITION BY seq_no ORDER BY move_type_max_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROUND(AVG(unit_price_decile) OVER (PARTITION BY seq_no ORDER BY move_type_max_unit_price, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_decile_avg_1_prior,
        RANK() OVER w_warehouse_id_sku_id_by_unit AS unit_price_decile_rank,
        MAX(move_type_max_unit_price) OVER (PARTITION BY operator) AS operator_max_move_type_max_unit_price
    FROM with_trends
    WINDOW w_warehouse_id_sku_id_by_unit AS (PARTITION BY warehouse_id, sku_id ORDER BY unit_price_decile)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        operator_clean,
        qty_plus_unit_price,
        unit_price_decile,
        move_type_max_unit_price,
        seq_no,
        unit_price_decile_avg_1_prior,
        ROUND(AVG(unit_price_decile) OVER (ORDER BY unit_price_decile, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_decile_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY unit_price_decile, id) AS unit_price_decile_seq,
        ROUND(AVG(move_type_max_unit_price) OVER (ORDER BY unit_price_decile, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS move_type_max_unit_price_avg_2_prior,
        seq_no - LAG(seq_no) OVER (ORDER BY unit_price_decile, id) AS seq_no_change,
        COUNT(*) OVER w_distinct_move_type_in_operat AS rows_in_distinct_move_type_in_operator
    FROM with_windows
    WINDOW w_distinct_move_type_in_operat AS (PARTITION BY distinct_move_type_in_operator)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    qty_plus_unit_price,
    unit_price_decile,
    move_type_max_unit_price,
    seq_no,
    unit_price_decile_avg_2_prior,
    unit_price_decile_seq,
    move_type_max_unit_price_avg_2_prior,
    seq_no_change,
    rows_in_distinct_move_type_in_operator,
    FLOOR(unit_price / 100) * 100 AS unit_price_bucket
FROM metrics
ORDER BY unit_price_decile_avg_2_prior DESC NULLS FIRST, id
LIMIT 10
