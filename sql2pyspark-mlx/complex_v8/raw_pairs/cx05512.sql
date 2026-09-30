WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE operator NOT IN ('op_bee', 'op_chan')
      AND move_type <> 'INBOUND'
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.move_type_slug,
        w.city AS warehouse_city,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        qty,
        unit_price,
        move_type_slug,
        sku_labels,
        sku_is_active,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        NTILE(5) OVER (PARTITION BY warehouse_city, sku_id ORDER BY unit_price, id) AS unit_price_quintile,
        MIN(unit_price) OVER w_move_type AS move_type_min_unit_price,
        NTILE(3) OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY qty, id) AS qty_tercile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type), 0), 4) AS unit_price_share_of_move_type,
        ROW_NUMBER() OVER w_warehouse_city_sku_id_by_uni AS unit_price_seq,
        qty - LAG(qty) OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY qty, id) AS qty_change,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id), 2) AS unit_price_vs_warehouse_id_sku_id_avg,
        MAX(unit_price) OVER (PARTITION BY move_type, sku_id) AS move_type_sku_id_max_unit_price,
        DENSE_RANK() OVER w_warehouse_city_sku_id_by_uni_2 AS unit_price_dense_rank,
        LEAD(unit_price, 2, 0) OVER (PARTITION BY warehouse_city, move_type ORDER BY qty, id) AS next2_unit_price,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type
    FROM with_details_warehouse
    WINDOW w_move_type AS (PARTITION BY move_type), w_warehouse_city_sku_id_by_uni AS (PARTITION BY warehouse_city, sku_id ORDER BY unit_price, id), w_warehouse_city_sku_id_by_uni_2 AS (PARTITION BY warehouse_city, sku_id ORDER BY unit_price)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE unit_price_dense_rank <= 5
      AND warehouse_id >= 3
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        qty_tercile,
        CASE WHEN qty_change >= 150 THEN 'A' WHEN qty_change >= -150 THEN 'B' ELSE 'C' END AS qty_change_tier,
        warehouse_id,
        ROUND(SUM(qty_tercile) / NULLIF(SUM(unit_price), 0), 3) AS qty_tercile_to_unit_price_ratio,
        ROUND(SUM(unit_price_quintile) / NULLIF(SUM(next2_unit_price), 0), 3) AS unit_price_quintile_to_next2_unit_price_ratio
    FROM top_per_group
    GROUP BY 1, 2, 3
)
SELECT
    qty_tercile,
    qty_change_tier,
    qty_tercile_to_unit_price_ratio,
    unit_price_quintile_to_next2_unit_price_ratio,
    ABS(qty_tercile - 4) AS qty_tercile_distance,
    FLOOR(qty_tercile / 2) * 2 AS qty_tercile_bucket
FROM rollup_base
ORDER BY qty_change_tier NULLS LAST
