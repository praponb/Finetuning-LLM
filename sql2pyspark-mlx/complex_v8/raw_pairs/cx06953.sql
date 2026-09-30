WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND qty > 145
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.operator_prefix,
        s.qty_bucket,
        w.wh_code AS warehouse_wh_code,
        sk.weight_kg AS sku_weight_kg
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        ROUND(qty_bucket - AVG(qty_bucket) OVER (PARTITION BY operator), 2) AS qty_bucket_vs_operator_avg,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id ORDER BY qty_bucket DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        qty_bucket - LAG(qty_bucket) OVER (PARTITION BY warehouse_id ORDER BY sku_weight_kg DESC, id) AS qty_bucket_change,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY operator, move_type ORDER BY sku_weight_kg DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sku_weight_kg_avg_3,
        LEAD(moved_at) OVER (PARTITION BY operator, YEAR(moved_at) ORDER BY moved_at DESC, id) AS next_moved_at,
        ROUND(CUME_DIST() OVER (ORDER BY qty_bucket, id), 3) AS qty_bucket_cume_dist,
        NTILE(5) OVER (PARTITION BY operator, YEAR(moved_at) ORDER BY moved_at DESC, id) AS moved_at_quintile,
        ROUND(PERCENT_RANK() OVER w_warehouse_id_by_sku_weight_k_2, 3) AS sku_weight_kg_percent_rank,
        DENSE_RANK() OVER w_operator_move_type_by_sku_we_2 AS sku_weight_kg_dense_rank,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER w_operator, 0), 4) AS sku_weight_kg_share_of_operator,
        ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY sku_weight_kg DESC, id) AS rn,
        MAX(qty_bucket) OVER (PARTITION BY sku_id) AS sku_id_max_qty_bucket
    FROM joined
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_warehouse_id_by_sku_weight_k_2 AS (PARTITION BY warehouse_id ORDER BY sku_weight_kg DESC), w_operator_move_type_by_sku_we_2 AS (PARTITION BY operator, move_type ORDER BY sku_weight_kg DESC), w_operator AS (PARTITION BY operator)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        operator_prefix,
        qty_bucket,
        warehouse_wh_code,
        sku_weight_kg,
        rows_in_sku_id,
        qty_bucket_vs_operator_avg,
        second_moved_at,
        qty_bucket_change,
        sku_weight_kg_avg_3,
        next_moved_at,
        qty_bucket_cume_dist,
        moved_at_quintile,
        sku_weight_kg_percent_rank,
        sku_weight_kg_share_of_operator,
        rn,
        sku_id_max_qty_bucket
    FROM sequenced
    WHERE sku_weight_kg_dense_rank <= 2
      AND warehouse_wh_code IS NOT NULL
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    operator,
    operator_prefix,
    qty_bucket,
    warehouse_wh_code,
    rows_in_sku_id,
    qty_bucket_vs_operator_avg,
    second_moved_at,
    next_moved_at,
    moved_at_quintile,
    sku_weight_kg_percent_rank,
    rn,
    LOWER(TRIM(warehouse_wh_code)) AS warehouse_wh_code_clean,
    MONTH(next_moved_at) AS next_moved_at_month
FROM top_rows
WHERE sku_weight_kg BETWEEN 1.8 AND 21.0
ORDER BY warehouse_wh_code_clean DESC NULLS FIRST, warehouse_id DESC NULLS FIRST
