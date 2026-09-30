WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-12 14:00:00'
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price,
        st.last_moved_at
    FROM recent_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(moved_at) AS moved_at_year_2,
        ABS(unit_price - 313.8) AS unit_price_distance,
        DAYOFWEEK(last_moved_at) AS last_moved_at_dayofweek
    FROM with_stats
),
labeled AS (
    -- Label and score rows
    SELECT *, TO_DATE(moved_at) AS moved_at_date
    FROM annotated
),
without_matches AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.unit_cost > 158.7
              AND NOT ARRAY_CONTAINS(sk.labels, 'hazmat')
        )
)
SELECT
    sku_id,
    moved_at_year,
    moved_at_year_2,
    GROUPING(sku_id) AS is_total_sku_id,
    MAX(unit_price_distance) AS max_unit_price_distance,
    PERCENTILE_APPROX(avg_move_unit_price, 0.5) AS median_avg_move_unit_price,
    ROUND(SUM(qty) / NULLIF(SUM(moved_at_year), 0), 3) AS qty_to_moved_at_year_ratio
FROM without_matches
GROUP BY GROUPING SETS ((sku_id, moved_at_year, moved_at_year_2), (sku_id), (moved_at_year))
ORDER BY sku_id NULLS LAST, moved_at_year NULLS LAST, moved_at_year_2 NULLS LAST
