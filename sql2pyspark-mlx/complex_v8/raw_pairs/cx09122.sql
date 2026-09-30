WITH recent AS (
    -- Scope the stock_moves rows
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
    WHERE sku_id <= 17
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM recent s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(qty, 3) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS next3_qty,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        SUM(qty) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        MAX(qty) OVER (PARTITION BY sku_id) AS sku_id_max_qty,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg,
        ROUND(AVG(qty) OVER (ORDER BY unit_price ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_avg_7,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY sku_id ORDER BY sku_unit_cost ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS sku_weight_kg_avg_6_prior,
        FIRST_VALUE(sku_weight_kg) OVER (PARTITION BY sku_id ORDER BY sku_unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_weight_kg,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        qty - LAG(qty) OVER (PARTITION BY operator ORDER BY sku_unit_cost DESC, id) AS qty_change,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER (PARTITION BY sku_id), 0), 4) AS sku_unit_cost_share_of_sku_id,
        FIRST_VALUE(unit_price) OVER (ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        MAX(sku_weight_kg) OVER (ORDER BY unit_price ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sku_weight_kg_max_7,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_price_ties, 3) AS unit_price_percent_rank
    FROM enriched
    WINDOW w_all_by_unit_price_ties AS (ORDER BY unit_price)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(second_sku_weight_kg, sku_weight_kg) AS greatest_second_sku_weight_kg_sku_weight_kg
    FROM with_trends
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    sku_weight_kg,
    next3_qty,
    running_qty,
    sku_id_max_qty,
    qty_avg_7,
    sku_weight_kg_avg_6_prior,
    first_sku_weight_kg,
    qty_vs_sku_id_avg,
    qty_change,
    sku_unit_cost_share_of_sku_id,
    first_unit_price,
    second_moved_at,
    first_known_moved_at,
    unit_price_percent_rank,
    greatest_second_sku_weight_kg_sku_weight_kg
FROM scored
ORDER BY sku_weight_kg NULLS LAST, id
LIMIT 20
