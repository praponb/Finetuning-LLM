WITH scoped AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MIN(moved_at) AS first_moved_at,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MIN(qty) AS min_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        st.avg_move_unit_price,
        st.min_move_qty
    FROM scoped s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY sku_id ORDER BY avg_move_unit_price ASC, id) AS avg_move_unit_price_decile,
        ROUND(avg_move_unit_price - AVG(avg_move_unit_price) OVER w_sku_id, 2) AS avg_move_unit_price_vs_sku_id_avg,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id ORDER BY avg_move_unit_price ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4
    FROM summarized_rows
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        operator,
        sku_id,
        SUM(qty_avg_4) FILTER (WHERE move_type IN ('TRANSFER', 'OUTBOUND')) AS qty_avg_4_move_type_in_transfer_outbou,
        MAX(avg_move_unit_price) AS max_avg_move_unit_price,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(*) AS row_count,
        ROUND(AVG(avg_move_unit_price), 2) AS avg_avg_move_unit_price
    FROM with_trends
    GROUP BY 1, 2, 3
    HAVING AVG(qty) > 123
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM per_group s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
)
SELECT
    warehouse_id,
    sku_id,
    qty_avg_4_move_type_in_transfer_outbou,
    max_avg_move_unit_price,
    distinct_move_types,
    move_type_list,
    row_count,
    SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
FROM without_matches
ORDER BY distinct_move_types, operator_prefix
