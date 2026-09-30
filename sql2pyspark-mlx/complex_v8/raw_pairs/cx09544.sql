WITH valid AS (
    -- Scope the skus rows
    SELECT
        id,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE (is_active = FALSE OR unit_cost BETWEEN 134.3 AND 251.2)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid
    WHERE unit_cost > (SELECT AVG(unit_cost) FROM valid)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator = 'op_eve') FROM stock_moves st WHERE st.sku_id = s.id) AS move_operator_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at
    FROM qualified s
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.operator NOT IN ('op_chan', 'op_dew')
              AND st.move_type = 'OUTBOUND'
        )
      AND SIZE(s.labels) >= 1
),
stats AS (
    -- Group-level totals
    SELECT
        is_active,
        YEAR(last_moved_at) AS last_moved_at_year,
        move_operator_count,
        MIN(move_operator_count) AS min_move_operator_count,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(unit_cost, 0.5) AS median_unit_cost,
        MIN(last_moved_at) AS first_last_moved_at,
        ROUND(SUM(move_operator_count) / NULLIF(SUM(weight_kg), 0), 3) AS move_operator_count_to_weight_kg_ratio
    FROM filtered
    GROUP BY is_active, YEAR(last_moved_at), move_operator_count
),
annotated AS (
    -- Derive helper columns
    SELECT
        is_active,
        last_moved_at_year,
        move_operator_count,
        min_move_operator_count,
        distinct_ids,
        median_unit_cost,
        first_last_moved_at,
        move_operator_count_to_weight_kg_ratio,
        QUARTER(first_last_moved_at) AS first_last_moved_at_quarter
    FROM stats
)
SELECT
    is_active,
    last_moved_at_year,
    move_operator_count,
    min_move_operator_count,
    distinct_ids,
    move_operator_count_to_weight_kg_ratio
FROM annotated
ORDER BY move_operator_count_to_weight_kg_ratio NULLS LAST, distinct_ids
