WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        sk.is_active AS sku_is_active,
        w.capacity AS warehouse_capacity
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        sku_is_active,
        DENSE_RANK() OVER (PARTITION BY operator, sku_is_active ORDER BY warehouse_capacity DESC) AS warehouse_capacity_dense_rank,
        warehouse_capacity - LAG(warehouse_capacity) OVER w_warehouse_id_by_unit_price AS warehouse_capacity_change,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, operator), 2) AS unit_price_vs_warehouse_id_operator_avg,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER w_sku_id, 2) AS warehouse_capacity_vs_sku_id_avg,
        FIRST_VALUE(warehouse_capacity) OVER (PARTITION BY warehouse_id ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_warehouse_capacity,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id, operator)) AS distinct_sku_id_in_warehouse_id_operator,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER w_warehouse_id_operator, 2) AS warehouse_capacity_vs_warehouse_id_operator_avg,
        NTH_VALUE(warehouse_capacity, 2) OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_capacity
    FROM with_details_sku
    WINDOW w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price DESC, id), w_sku_id AS (PARTITION BY sku_id), w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        sku_is_active,
        warehouse_capacity_dense_rank,
        warehouse_capacity_change,
        unit_price_vs_warehouse_id_operator_avg,
        warehouse_capacity_vs_sku_id_avg,
        first_warehouse_capacity,
        distinct_sku_id_in_warehouse_id_operator,
        warehouse_capacity_vs_warehouse_id_operator_avg,
        warehouse_capacity_change - LAG(warehouse_capacity_change) OVER (ORDER BY warehouse_capacity_dense_rank, id) AS warehouse_capacity_change_change,
        ROUND(CUME_DIST() OVER (ORDER BY warehouse_capacity_dense_rank), 3) AS warehouse_capacity_dense_rank_cume_dist,
        ROW_NUMBER() OVER w_all_by_warehouse_capacity_de AS warehouse_capacity_dense_rank_seq,
        ROUND(AVG(warehouse_capacity_change) OVER (PARTITION BY sku_id ORDER BY warehouse_capacity_vs_sku_id_avg, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_change_avg_2,
        ROUND(AVG(second_warehouse_capacity) OVER (PARTITION BY warehouse_capacity_dense_rank, sku_is_active), 2) AS warehouse_capacity_dense_rank_sku_is_active_avg_,
        MAX(first_warehouse_capacity) OVER (ORDER BY warehouse_capacity_dense_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS first_warehouse_capacity_max_6_prior
    FROM with_trends
    WINDOW w_all_by_warehouse_capacity_de AS (ORDER BY warehouse_capacity_dense_rank, id)
)
SELECT
    id,
    sku_id,
    sku_is_active,
    warehouse_capacity_dense_rank,
    warehouse_capacity_change,
    unit_price_vs_warehouse_id_operator_avg,
    warehouse_capacity_vs_sku_id_avg,
    distinct_sku_id_in_warehouse_id_operator,
    warehouse_capacity_vs_warehouse_id_operator_avg,
    warehouse_capacity_change_change,
    warehouse_capacity_dense_rank_cume_dist,
    warehouse_capacity_change_avg_2,
    warehouse_capacity_dense_rank_sku_is_active_avg_,
    first_warehouse_capacity_max_6_prior
FROM windowed
ORDER BY warehouse_capacity_vs_sku_id_avg DESC, warehouse_capacity_vs_warehouse_id_operator_avg
