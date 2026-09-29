WITH valid AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE unit_price > 205.1
      AND qty < 384
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM valid
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg
    FROM enriched s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
enriched_sku AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg_2,
        sk.unit_cost AS sku_unit_cost
    FROM joined s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
joined_2 AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.qty_per_unit_price,
        s.operator_or_unknown,
        s.warehouse_capacity,
        s.warehouse_city,
        s.warehouse_wh_code,
        s.sku_code,
        s.sku_weight_kg,
        s.sku_weight_kg_2,
        sk.labels AS sku_labels_2,
        sk.category AS sku_category,
        w.capacity AS warehouse_capacity_2,
        w.opened_on AS warehouse_opened_on
    FROM enriched_sku s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE NOT ARRAY_CONTAINS(sk.labels, 'fragile')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        qty_per_unit_price,
        operator_or_unknown,
        warehouse_city,
        warehouse_wh_code,
        sku_weight_kg,
        sku_labels_2,
        sku_category,
        warehouse_capacity_2,
        MAX(sku_weight_kg_2) OVER (PARTITION BY move_type) AS move_type_max_sku_weight_kg_2,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type) AS distinct_warehouse_id_in_move_type,
        NTILE(5) OVER (PARTITION BY sku_category, warehouse_city ORDER BY warehouse_capacity_2 DESC NULLS FIRST, id) AS warehouse_capacity_2_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY qty_per_unit_price DESC, id), 3) AS qty_per_unit_price_percent_rank,
        MAX(warehouse_capacity_2) OVER (PARTITION BY sku_id ORDER BY qty_per_unit_price DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS warehouse_capacity_2_max_4
    FROM joined_2
    WINDOW w_move_type AS (PARTITION BY move_type)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    qty_per_unit_price,
    operator_or_unknown,
    warehouse_city,
    warehouse_capacity_2_quintile,
    qty_per_unit_price_percent_rank,
    warehouse_capacity_2_max_4
FROM with_trends
ORDER BY warehouse_id DESC, move_type, id
LIMIT 20
