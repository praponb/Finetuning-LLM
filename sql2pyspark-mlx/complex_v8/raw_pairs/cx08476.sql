WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE unit_price > 200.7
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_max_unit_price,
        ROUND(AVG(unit_price) OVER (ORDER BY qty ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        MAX(unit_price) OVER (ORDER BY qty ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id, move_type ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        NTILE(10) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price DESC, id) AS unit_price_decile,
        unit_price - LAG(unit_price) OVER (ORDER BY qty DESC, id) AS unit_price_change,
        MIN(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, move_type ORDER BY qty, id), 3) AS qty_cume_dist,
        ROUND(AVG(qty) OVER (PARTITION BY move_type, sku_id), 2) AS move_type_sku_id_avg_qty,
        MIN(unit_price) OVER (PARTITION BY warehouse_id, move_type ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far
    FROM scoped_moves
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        move_type_warehouse_id_max_unit_price,
        unit_price_avg_7,
        unit_price_max_2_prior,
        unit_price_decile,
        unit_price_change,
        qty_cume_dist,
        min_unit_price_so_far,
        NTILE(3) OVER (PARTITION BY move_type, unit_price_decile ORDER BY unit_price_max_2_prior, id) AS unit_price_max_2_prior_tercile,
        NTILE(10) OVER (PARTITION BY move_type ORDER BY qty, id) AS qty_decile,
        ROUND(qty - AVG(qty) OVER w_unit_price_decile_sku_id, 2) AS qty_vs_unit_price_decile_sku_id_avg,
        FIRST_VALUE(warehouse_id_min_unit_price) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_warehouse_id_min_unit_price,
        COUNT(*) OVER (PARTITION BY unit_price_decile, sku_id) AS rows_in_unit_price_decile_sku_id
    FROM with_trends
    WINDOW w_unit_price_decile_sku_id AS (PARTITION BY unit_price_decile, sku_id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    unit_price_avg_7,
    unit_price_max_2_prior,
    unit_price_change,
    min_unit_price_so_far,
    unit_price_max_2_prior_tercile,
    qty_decile,
    qty_vs_unit_price_decile_sku_id_avg,
    first_known_warehouse_id_min_unit_price,
    rows_in_unit_price_decile_sku_id,
    SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
FROM with_trends_2
ORDER BY qty_decile DESC, unit_price_max_2_prior
