WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        operator_ref,
        days_since_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY moved_at DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(unit_price) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER (PARTITION BY move_type, operator), 2) AS days_since_moved_at_vs_move_type_operator_avg,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, operator), 2) AS unit_price_vs_move_type_operator_avg,
        COUNT(*) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER (PARTITION BY move_type, operator), 0), 4) AS days_since_moved_at_share_of_move_type_operator
    FROM latest_per_key
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN days_since_moved_at >= 595 THEN 'A' WHEN days_since_moved_at >= 210 THEN 'B' ELSE 'C' END AS days_since_moved_at_tier
    FROM metrics
)
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    operator,
    unit_price,
    operator_ref,
    days_since_moved_at,
    last_unit_price,
    days_since_moved_at_vs_move_type_operator_avg,
    unit_price_vs_move_type_operator_avg,
    days_since_moved_at_share_of_move_type_operator,
    days_since_moved_at_tier,
    CASE WHEN running_count >= 17 THEN 'large' WHEN running_count >= 6 THEN 'medium' ELSE 'small' END AS running_count_tier,
    DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at_2
FROM enriched
ORDER BY id
