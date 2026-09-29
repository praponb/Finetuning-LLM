WITH scoped_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        labels,
        is_active,
        SUBSTRING(TRIM(sku_code), 1, 2) AS sku_code_prefix,
        GREATEST(unit_cost, weight_kg) AS greatest_unit_cost_weight_kg
    FROM skus
    WHERE ARRAY_CONTAINS(labels, 'cold')
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at
    FROM scoped_skus s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE s.id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.move_type NOT IN ('OUTBOUND', 'INBOUND')
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(sku_code_prefix)) AS sku_code_prefix_len,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM without_matches
    WHERE is_active = FALSE
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.sku_id = s.id) AS total_move_qty,
        (SELECT COUNT(CASE WHEN st.operator <> 'op_eve' THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.id) AS move_operator_count
    FROM scored s
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_counts
    WHERE total_move_qty < (SELECT MAX(total_move_qty) FROM with_counts)
)
SELECT
    id,
    sku_code,
    labels,
    is_active,
    sku_code_prefix,
    greatest_unit_cost_weight_kg,
    last_moved_at,
    sku_code_prefix_len,
    total_move_qty,
    move_operator_count,
    LEAST(total_move_qty, move_operator_count) AS least_total_move_qty_move_operator_count,
    LOWER(REGEXP_REPLACE(TRIM(sku_code_prefix), ' ', '_')) AS sku_code_prefix_slug
FROM screened
ORDER BY sku_code_prefix_len DESC
