WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        ABS(unit_price - 290.4) AS unit_price_distance
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND operator NOT IN ('op_dew', 'op_eve')
),
without_matches AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM filtered_moves s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.unit_cost <= 306.4
              AND NOT sk.is_active
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.operator_len,
        s.qty_bucket,
        sk.sku_code,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM without_matches s
    JOIN skus sk ON sk.id = s.sku_id AND UPPER(TRIM(sk.sku_code)) LIKE 'T%'
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(sk.sku_code)) LIKE 'E%'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY move_type), 0), 4) AS qty_share_of_move_type,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type)) AS distinct_sku_id_in_move_type
    FROM combined_details
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        operator_len,
        qty_bucket,
        sku_code,
        warehouse_wh_code,
        qty_share_of_move_type,
        second_unit_price,
        distinct_sku_id_in_move_type,
        ROUND(distinct_sku_id_in_move_type - AVG(distinct_sku_id_in_move_type) OVER (PARTITION BY MONTH(moved_at), distinct_sku_id_in_move_type), 2) AS distinct_sku_id_in_move_type_vs_moved_at_month_d,
        NTH_VALUE(second_unit_price, 2) OVER (PARTITION BY warehouse_city ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_unit_price,
        ROUND(AVG(second_unit_price) OVER (PARTITION BY MONTH(moved_at), distinct_sku_id_in_move_type), 2) AS moved_at_month_distinct_sku_id_in_move_type_avg_,
        COUNT(*) OVER w_moved_at_month_distinct_sku AS rows_in_moved_at_month_distinct_sku_id_in_move_t,
        ROUND(qty_share_of_move_type / NULLIF(SUM(qty_share_of_move_type) OVER (PARTITION BY MONTH(moved_at), distinct_sku_id_in_move_type), 0), 4) AS qty_share_of_move_type_share_of_moved_at_month_d
    FROM windowed
    WINDOW w_moved_at_month_distinct_sku AS (PARTITION BY MONTH(moved_at), distinct_sku_id_in_move_type)
),
qualified AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.unit_cost < 306.4
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    unit_price,
    operator_len,
    qty_bucket,
    sku_code,
    second_unit_price,
    distinct_sku_id_in_move_type_vs_moved_at_month_d,
    moved_at_month_distinct_sku_id_in_move_type_avg_,
    rows_in_moved_at_month_distinct_sku_id_in_move_t,
    qty_share_of_move_type_share_of_moved_at_month_d
FROM qualified
