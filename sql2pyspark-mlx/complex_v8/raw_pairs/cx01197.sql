WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty > 102
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        LENGTH(TRIM(operator)) AS operator_len,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM scoped_moves
    WHERE move_type <> 'TRANSFER'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        CASE WHEN move_type IN ('INBOUND', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM derived
    WHERE move_type IN ('ADJUST', 'INBOUND', 'TRANSFER')
),
qualified AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND UPPER(TRIM(w.wh_code)) LIKE 'B%'
              AND w.opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-12-17'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    sku_id,
    COALESCE(move_type_group, 'ALL') AS move_type_group,
    GROUPING_ID() AS gid,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE move_type IS NOT NULL) AS n_move_type_is_not_null
FROM qualified
GROUP BY GROUPING SETS ((sku_id, move_type_group), (sku_id), ())
ORDER BY sku_id NULLS LAST, move_type_group NULLS LAST
