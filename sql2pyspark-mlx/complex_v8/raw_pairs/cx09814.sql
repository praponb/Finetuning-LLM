WITH recent_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        ARRAY_CONTAINS(labels, 'cold') AS has_cold
    FROM skus
    WHERE weight_kg <= 14.6
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent_skus s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
        )
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MIN(moved_at) AS first_moved_at,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) FILTER (WHERE move_type IN ('INBOUND', 'OUTBOUND')) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        st.last_moved_at,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM with_activity s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(last_moved_at) AS last_moved_at_day,
        YEAR(first_moved_at) AS first_moved_at_year,
        has_cold,
        MIN(move_move_type_count) AS min_move_move_type_count,
        COUNT(*) FILTER (WHERE category IN ('Beauty', 'Apparel', 'Toys')) AS n_category_in_beauty_apparel_t
    FROM summarized_rows
    GROUP BY TO_DATE(last_moved_at), YEAR(first_moved_at), has_cold
)
SELECT
    last_moved_at_day,
    first_moved_at_year,
    has_cold,
    min_move_move_type_count,
    n_category_in_beauty_apparel_t,
    min_move_move_type_count - first_moved_at_year AS min_move_move_type_count_minus_first_moved_at_ye,
    ABS(first_moved_at_year - 2025) AS first_moved_at_year_distance
FROM rollup_base
