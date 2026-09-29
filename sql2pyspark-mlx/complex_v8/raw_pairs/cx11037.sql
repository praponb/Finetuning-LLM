WITH scoped_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        GREATEST(weight_kg, unit_cost) AS greatest_weight_kg_unit_cost
    FROM skus
    WHERE NOT is_active
      AND sku_code IS NOT NULL
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.category,
        s.greatest_weight_kg_unit_cost,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type,
        st.qty AS move_qty
    FROM scoped_skus s
    JOIN stock_moves st ON st.sku_id = s.id
),
enriched AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(category)) AS category_len
    FROM detail_rows
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MAX(moved_at) AS last_moved_at, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        COALESCE(st.move_count, 0) AS move_count
    FROM enriched s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        category,
        greatest_weight_kg_unit_cost,
        move_id,
        move_unit_price,
        category_len,
        last_moved_at,
        move_count,
        LAST_VALUE(move_qty) OVER (PARTITION BY move_type, move_id ORDER BY greatest_weight_kg_unit_cost DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_move_qty,
        COUNT(*) OVER w_category_len AS rows_in_category_len,
        RANK() OVER (PARTITION BY move_type, move_id ORDER BY greatest_weight_kg_unit_cost DESC, id, move_id) AS greatest_weight_kg_unit_cost_rank,
        SIZE(COLLECT_SET(move_id) OVER (PARTITION BY category_len)) AS distinct_move_id_in_category_len,
        NTH_VALUE(move_unit_price, 2) OVER (PARTITION BY move_type, move_id ORDER BY greatest_weight_kg_unit_cost DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_unit_price,
        ROUND(AVG(category_len) OVER (PARTITION BY category_len), 2) AS category_len_avg_category_len
    FROM with_counts
    WINDOW w_category_len AS (PARTITION BY category_len)
)
SELECT
    id,
    greatest_weight_kg_unit_cost,
    move_id,
    move_unit_price,
    category_len,
    move_count,
    last_move_qty,
    rows_in_category_len,
    greatest_weight_kg_unit_cost_rank
FROM metrics
ORDER BY id DESC, last_move_qty DESC, move_id
LIMIT 10
