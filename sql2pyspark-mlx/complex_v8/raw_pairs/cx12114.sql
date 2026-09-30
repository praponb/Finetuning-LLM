WITH recent AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE warehouse_id <= 2
      AND unit_price >= 205.1
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
    WHERE LOWER(w.wh_code) LIKE '%x1%'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY qty DESC, id) AS qty_dense_rank,
        MAX(warehouse_capacity) OVER w_sku_id_operator AS sku_id_operator_max_warehouse_capacity,
        NTH_VALUE(warehouse_capacity, 2) OVER (PARTITION BY sku_id, sku_category ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_capacity,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER w_sku_category_operator, 2) AS warehouse_capacity_vs_sku_category_operator_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id), 3) AS unit_price_percent_rank,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id, operator), 2) AS qty_vs_sku_id_operator_avg,
        LEAD(warehouse_capacity) OVER (PARTITION BY operator ORDER BY qty DESC, id) AS next_warehouse_capacity,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY sku_id, sku_category ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LAST_VALUE(warehouse_capacity) IGNORE NULLS OVER (PARTITION BY warehouse_id, operator ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_capacity,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_3_prior,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY sku_category, operator), 2) AS sku_unit_cost_vs_sku_category_operator_avg,
        COUNT(*) OVER (PARTITION BY sku_category, operator) AS rows_in_sku_category_operator
    FROM combined_details_warehouse
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator), w_sku_category_operator AS (PARTITION BY sku_category, operator)
)
SELECT
    id,
    sku_id,
    qty,
    unit_price,
    warehouse_capacity,
    sku_category,
    qty_dense_rank,
    sku_id_operator_max_warehouse_capacity,
    warehouse_capacity_vs_sku_category_operator_avg,
    unit_price_percent_rank,
    next_warehouse_capacity,
    last_known_warehouse_capacity,
    sku_unit_cost_avg_3_prior,
    sku_unit_cost_vs_sku_category_operator_avg,
    rows_in_sku_category_operator,
    ROUND(unit_price / NULLIF(next_warehouse_capacity, 0), 3) AS unit_price_per_next_warehouse_capacity,
    FLOOR(qty_dense_rank / 2) * 2 AS qty_dense_rank_bucket
FROM ranked
WHERE sku_id_operator_max_warehouse_capacity >= 40800
ORDER BY sku_unit_cost_avg_3_prior, sku_id_operator_max_warehouse_capacity DESC NULLS FIRST
