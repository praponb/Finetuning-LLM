WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        QUARTER(moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE move_type NOT IN ('TRANSFER', 'OUTBOUND')
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        operator,
        move_type,
        ROUND(100 * COUNT(CASE WHEN move_type IN ('OUTBOUND', 'ADJUST') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_outbound_adjust,
        COUNT(*) AS cnt
    FROM filtered
    GROUP BY sku_id, operator, move_type
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(cnt - 6) AS cnt_distance,
        CASE WHEN cnt >= 10 THEN 'A' WHEN cnt >= 5 THEN 'B' ELSE 'C' END AS cnt_tier,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM summary
    WHERE sku_id >= 8
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.sku_id,
        s.operator,
        s.move_type,
        s.pct_move_type_in_outbound_adjust,
        s.operator_slug,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.move_type AS move_move_type
    FROM enriched s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    sku_id,
    operator,
    pct_move_type_in_outbound_adjust,
    operator_slug,
    move_move_type,
    CASE WHEN move_move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_move_type_group
FROM detail_rows_moves
WHERE move_id <= 99
ORDER BY move_move_type DESC, operator_slug
