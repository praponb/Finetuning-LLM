WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE warehouse_id >= 2
      AND move_type NOT IN ('INBOUND', 'TRANSFER')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at_dayofweek,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM valid_moves
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM derived s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count, COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM profiled s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(unit_price_adj, move_count) AS greatest_unit_price_adj_move_count,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM with_counts
    WHERE qty >= 112
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        id,
        qty,
        operator,
        unit_price,
        moved_at_dayofweek,
        greatest_qty_unit_price,
        unit_price_adj,
        move_count,
        move_count_2,
        greatest_unit_price_adj_move_count,
        moved_at_hour,
        moved_at_week,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM enriched
),
operator_stats AS (
    -- Median greatest_unit_price_adj_move_count per operator
    SELECT operator, PERCENTILE_APPROX(greatest_unit_price_adj_move_count, 0.5) AS median_greatest_unit_price_adj_move_count
    FROM derived_2
    GROUP BY operator
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_greatest_unit_price_adj_move_count,
        ROUND(s.greatest_unit_price_adj_move_count - g.median_greatest_unit_price_adj_move_count, 2) AS greatest_unit_price_adj_move_count_minus_median
    FROM derived_2 s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.greatest_unit_price_adj_move_count >= g.median_greatest_unit_price_adj_move_count
)
-- Final projection
SELECT
    id,
    qty,
    operator,
    moved_at_dayofweek,
    unit_price_adj,
    move_count,
    move_count_2,
    greatest_unit_price_adj_move_count,
    moved_at_hour,
    moved_at_week,
    move_type_prefix,
    median_greatest_unit_price_adj_move_count
FROM vs_group
WHERE move_count_2 <= 5
