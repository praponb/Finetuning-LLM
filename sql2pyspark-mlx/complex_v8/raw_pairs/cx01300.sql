WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        weight_kg,
        labels,
        is_active,
        CONCAT(category, '#', CAST(id AS STRING)) AS category_ref
    FROM skus
    WHERE ARRAY_CONTAINS(labels, 'express')
      AND category NOT IN ('Beauty', 'Toys')
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.warehouse_id >= 3
              AND st.unit_price BETWEEN 58.6 AND 328.3
        )
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.weight_kg,
        s.labels,
        s.category_ref,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty
    FROM without_matches s
    LEFT JOIN stock_moves st ON st.sku_id = s.id AND st.moved_at >= TIMESTAMP '2025-01-31 08:00:00'
),
per_label AS (
    -- One row per label
    SELECT
        s.id,
        s.weight_kg,
        s.category_ref,
        s.move_id,
        s.move_moved_at,
        s.move_operator,
        s.move_warehouse_id,
        s.move_qty,
        t.label
    FROM with_children_moves s
    LATERAL VIEW EXPLODE(s.labels) t AS label
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        weight_kg,
        move_id,
        move_moved_at,
        move_operator,
        move_warehouse_id,
        move_qty,
        label,
        MONTH(move_moved_at) AS move_moved_at_month,
        COALESCE(label, 'UNKNOWN') AS label_or_unknown
    FROM per_label
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(move_moved_at_month / NULLIF(SUM(move_moved_at_month) OVER (PARTITION BY label), 0), 4) AS move_moved_at_month_share_of_label,
        MIN(move_moved_at_month) OVER (PARTITION BY label) AS label_min_move_moved_at_month,
        COUNT(*) OVER (PARTITION BY label) AS rows_in_label,
        MAX(move_moved_at_month) OVER (PARTITION BY move_moved_at_month ORDER BY move_qty DESC, id, move_id, label ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS move_moved_at_month_max_1_prior
    FROM tagged
    WINDOW w_label AS (PARTITION BY label)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.move_warehouse_id) AS move_count
    FROM windowed s
)
-- Final projection
SELECT
    id,
    weight_kg,
    move_id,
    move_moved_at,
    move_operator,
    move_warehouse_id,
    move_qty,
    move_moved_at_month,
    label_or_unknown,
    move_moved_at_month_share_of_label,
    label_min_move_moved_at_month,
    rows_in_label,
    move_moved_at_month_max_1_prior,
    move_count,
    CASE WHEN label IN ('bulky', 'cold') THEN 'group_a' WHEN label = 'express' THEN 'group_b' ELSE 'other' END AS label_group,
    ROUND(weight_kg * 0.25, 2) AS weight_kg_adj
FROM with_stats
WHERE move_moved_at_month_share_of_label > 0.3
ORDER BY move_warehouse_id
