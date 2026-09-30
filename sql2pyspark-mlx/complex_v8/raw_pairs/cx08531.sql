WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg
    FROM scoped_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.weight_kg > 12.4
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        operator,
        sku_id,
        ROUND(100 * COUNT(CASE WHEN operator IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_operator_is_not_null,
        COUNT(*) AS row_count,
        SUM(sku_weight_kg) FILTER (WHERE LENGTH(TRIM(sku_code)) >= 7) AS sku_weight_kg_length_trim_sku_code_7,
        COUNT(*) FILTER (WHERE UPPER(TRIM(sku_code)) LIKE 'T%') AS n_upper_trim_sku_code_like_t
    FROM joined_sku
    GROUP BY warehouse_id, operator, sku_id
)
SELECT
    warehouse_id,
    operator,
    pct_operator_is_not_null,
    sku_weight_kg_length_trim_sku_code_7,
    n_upper_trim_sku_code_like_t,
    LENGTH(TRIM(operator)) AS operator_len,
    COALESCE(row_count, 0) AS row_count_filled
FROM rollup_base
WHERE sku_id <= 14
ORDER BY operator_len DESC, n_upper_trim_sku_code_like_t DESC NULLS FIRST
