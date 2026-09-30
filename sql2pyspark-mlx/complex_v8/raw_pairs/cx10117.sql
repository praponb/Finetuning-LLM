WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id <= 7
      AND warehouse_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY operator ORDER BY warehouse_capacity DESC NULLS FIRST, id) AS warehouse_capacity_quartile,
        ROUND(sku_weight_kg - AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id, sku_id), 2) AS sku_weight_kg_vs_warehouse_id_sku_id_avg,
        MIN(qty) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_min_qty,
        NTILE(4) OVER (PARTITION BY sku_id, move_type ORDER BY warehouse_opened_on, id) AS warehouse_opened_on_quartile,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id_operator, 0), 4) AS qty_share_of_warehouse_id_operator,
        SUM(qty) OVER w_warehouse_id_operator AS warehouse_id_operator_sum_qty,
        LAG(unit_price) OVER (PARTITION BY sku_id, move_type ORDER BY warehouse_opened_on, id) AS prev_unit_price,
        MIN(unit_price) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_min_unit_price,
        MIN(unit_price) OVER (PARTITION BY operator ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        LAG(warehouse_capacity) OVER (PARTITION BY operator ORDER BY warehouse_capacity DESC NULLS FIRST, id) AS prev_warehouse_capacity,
        MAX(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_max_3,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, sku_id)) AS distinct_operator_in_warehouse_id_sku_id,
        LAST_VALUE(qty) IGNORE NULLS OVER (ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        FIRST_VALUE(unit_price) OVER (PARTITION BY operator ORDER BY warehouse_capacity DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY unit_price) AS unit_price_dense_rank
    FROM enriched
    WINDOW w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator)
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        sku_weight_kg,
        sku_category,
        sku_unit_cost,
        warehouse_opened_on,
        sku_weight_kg_vs_warehouse_id_sku_id_avg,
        warehouse_id_operator_min_qty,
        warehouse_opened_on_quartile,
        prev_unit_price,
        min_unit_price_so_far,
        distinct_operator_in_warehouse_id_sku_id,
        last_known_qty,
        first_unit_price,
        unit_price_dense_rank,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM with_trends
    WHERE warehouse_id >= 3
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    sku_weight_kg,
    sku_category,
    sku_unit_cost,
    warehouse_opened_on,
    sku_weight_kg_vs_warehouse_id_sku_id_avg,
    warehouse_id_operator_min_qty,
    prev_unit_price,
    min_unit_price_so_far,
    last_known_qty,
    first_unit_price,
    unit_price_dense_rank,
    moved_at_plus_30m,
    moved_at_label,
    ABS(first_unit_price - 255.6) AS first_unit_price_distance,
    TO_DATE(moved_at) AS moved_at_date
FROM enriched_2
WHERE sku_unit_cost > 164.5
ORDER BY first_unit_price DESC, prev_unit_price NULLS LAST
