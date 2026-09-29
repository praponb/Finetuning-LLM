WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE operator IN ('op_ann', 'op_chan')
      AND qty > 116
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_type_ref,
        w.wh_code AS warehouse_wh_code,
        sk.weight_kg AS sku_weight_kg
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        move_type_ref,
        sku_weight_kg,
        ROUND(PERCENT_RANK() OVER w_moved_at_year_by_unit_price, 3) AS unit_price_percent_rank,
        SUM(sku_weight_kg) OVER (ORDER BY sku_weight_kg, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sku_weight_kg_sum_3_prior,
        LAG(moved_at) OVER (PARTITION BY move_type ORDER BY sku_weight_kg, id) AS prev_moved_at,
        LAG(unit_price) OVER (PARTITION BY YEAR(moved_at) ORDER BY unit_price DESC, id) AS prev_unit_price,
        ROUND(AVG(unit_price) OVER w_sku_id_move_type, 2) AS sku_id_move_type_avg_unit_price,
        COUNT(*) OVER (PARTITION BY sku_id, move_type) AS rows_in_sku_id_move_type,
        ROUND(PERCENT_RANK() OVER (ORDER BY sku_weight_kg, id), 3) AS sku_weight_kg_percent_rank,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY sku_weight_kg, id), 3) AS sku_weight_kg_percent_rank_2,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER w_move_type_warehouse_id, 0), 4) AS sku_weight_kg_share_of_move_type_warehouse_id,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type), 2) AS unit_price_vs_move_type_avg,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id_move_type) AS distinct_warehouse_id_in_sku_id_move_type,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY YEAR(moved_at) ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS sku_weight_kg_avg_1_prior,
        ROUND(sku_weight_kg - AVG(sku_weight_kg) OVER w_move_type, 2) AS sku_weight_kg_vs_move_type_avg,
        RANK() OVER w_warehouse_id_sku_id_by_sku_w_2 AS sku_weight_kg_rank
    FROM enriched
    WINDOW w_moved_at_year_by_unit_price AS (PARTITION BY YEAR(moved_at) ORDER BY unit_price DESC), w_sku_id_move_type AS (PARTITION BY sku_id, move_type), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id), w_move_type AS (PARTITION BY move_type), w_warehouse_id_sku_id_by_sku_w_2 AS (PARTITION BY warehouse_id, sku_id ORDER BY sku_weight_kg)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(sku_weight_kg_percent_rank_2) OVER (PARTITION BY sku_weight_kg_rank ORDER BY distinct_warehouse_id_in_sku_id_move_type, id) AS next_sku_weight_kg_percent_rank_2,
        RANK() OVER w_sku_weight_kg_rank_by_distin_2 AS distinct_warehouse_id_in_sku_id_move_type_rank,
        ROUND(AVG(sku_weight_kg_percent_rank) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_sku_weight_kg_percent_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        RANK() OVER w_distinct_warehouse_id_in_sku AS rows_in_sku_id_move_type_rank
    FROM windowed
    WINDOW w_sku_weight_kg_rank_by_distin_2 AS (PARTITION BY sku_weight_kg_rank ORDER BY distinct_warehouse_id_in_sku_id_move_type), w_distinct_warehouse_id_in_sku AS (PARTITION BY distinct_warehouse_id_in_sku_id_move_type ORDER BY rows_in_sku_id_move_type)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    move_type_ref,
    sku_weight_kg,
    sku_weight_kg_sum_3_prior,
    prev_moved_at,
    prev_unit_price,
    sku_id_move_type_avg_unit_price,
    rows_in_sku_id_move_type,
    sku_weight_kg_percent_rank,
    rows_in_move_type,
    sku_weight_kg_percent_rank_2,
    sku_weight_kg_share_of_move_type_warehouse_id,
    unit_price_vs_move_type_avg,
    distinct_warehouse_id_in_sku_id_move_type,
    sku_weight_kg_avg_1_prior,
    sku_weight_kg_vs_move_type_avg,
    sku_weight_kg_rank,
    next_sku_weight_kg_percent_rank_2,
    distinct_warehouse_id_in_sku_id_move_type_rank,
    sku_id_avg_sku_weight_kg_percent_rank,
    distinct_warehouse_id_in_sku_id,
    ROUND(unit_price_percent_rank * 1.07, 2) AS unit_price_percent_rank_adj
FROM with_windows
ORDER BY sku_weight_kg_vs_move_type_avg DESC, sku_weight_kg_percent_rank_2 DESC, id
LIMIT 20
