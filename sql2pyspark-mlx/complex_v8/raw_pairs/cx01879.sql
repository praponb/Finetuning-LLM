WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type <> 'OUTBOUND'
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        w.city AS warehouse_city
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.category NOT IN ('Electronics', 'Beauty')
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_warehouse_id_warehouse_city AS sku_weight_kg_dense_rank,
        NTILE(5) OVER (PARTITION BY warehouse_id, warehouse_city ORDER BY sku_weight_kg DESC, id) AS sku_weight_kg_quintile,
        SUM(sku_unit_cost) OVER (PARTITION BY warehouse_id, warehouse_city ORDER BY sku_weight_kg DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sku_unit_cost_sum_4,
        MAX(sku_weight_kg) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_max_sku_weight_kg,
        ROUND(AVG(sku_unit_cost) OVER w_warehouse_id_operator, 2) AS warehouse_id_operator_avg_sku_unit_cost,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY sku_weight_kg DESC NULLS FIRST, id) AS seq_no,
        FIRST_VALUE(sku_weight_kg) OVER (PARTITION BY warehouse_id, warehouse_city ORDER BY sku_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_weight_kg,
        RANK() OVER (PARTITION BY warehouse_id, warehouse_city ORDER BY sku_weight_kg DESC, id) AS sku_weight_kg_rank
    FROM joined_sku
    WINDOW w_warehouse_id_warehouse_city AS (PARTITION BY warehouse_id, warehouse_city ORDER BY sku_weight_kg DESC), w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        sku_unit_cost,
        sku_weight_kg,
        warehouse_city,
        sku_weight_kg_quintile,
        sku_unit_cost_sum_4,
        warehouse_id_operator_max_sku_weight_kg,
        warehouse_id_operator_avg_sku_unit_cost,
        seq_no,
        first_sku_weight_kg,
        sku_weight_kg_rank
    FROM with_windows
    WHERE sku_weight_kg_dense_rank <= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        sku_weight_kg_quintile,
        warehouse_id_operator_max_sku_weight_kg,
        warehouse_id_operator_avg_sku_unit_cost,
        seq_no,
        first_sku_weight_kg,
        sku_weight_kg_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, sku_weight_kg_rank)) AS distinct_sku_id_in_move_type_sku_weight_kg_rank,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type, sku_weight_kg_rank)) AS distinct_operator_in_move_type_sku_weight_kg_ran,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY warehouse_city, seq_no ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS sku_weight_kg_avg_7,
        ROUND(PERCENT_RANK() OVER w_warehouse_city_seq_no_by_sku_2, 3) AS sku_unit_cost_percent_rank,
        ROUND(AVG(first_sku_weight_kg) OVER (PARTITION BY warehouse_city, seq_no ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS first_sku_weight_kg_avg_1_prior
    FROM top_rows
    WINDOW w_warehouse_city_seq_no_by_sku_2 AS (PARTITION BY warehouse_city, seq_no ORDER BY sku_unit_cost DESC)
)
SELECT
    id,
    sku_id,
    move_type,
    warehouse_id_operator_max_sku_weight_kg,
    warehouse_id_operator_avg_sku_unit_cost,
    seq_no,
    first_sku_weight_kg,
    sku_weight_kg_rank,
    distinct_operator_in_move_type_sku_weight_kg_ran,
    sku_weight_kg_avg_7,
    sku_unit_cost_percent_rank,
    first_sku_weight_kg_avg_1_prior,
    GREATEST(first_sku_weight_kg, sku_weight_kg_quintile) AS greatest_first_sku_weight_kg_sku_weight_kg_quint,
    ROUND(sku_weight_kg_avg_7 / NULLIF(seq_no, 0), 3) AS sku_weight_kg_avg_7_per_seq_no
FROM metrics
WHERE sku_weight_kg_rank >= 5
ORDER BY seq_no NULLS LAST, first_sku_weight_kg_avg_1_prior DESC
