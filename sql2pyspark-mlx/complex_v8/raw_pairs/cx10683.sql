WITH prepared_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter,
        CONCAT(wh_code, '#', CAST(id AS STRING)) AS wh_code_ref,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug
    FROM warehouses
    WHERE capacity >= 2614
      AND city IN ('Singapore', 'Kuala Lumpur')
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_warehouses
    WHERE capacity > (SELECT AVG(capacity) FROM prepared_warehouses)
),
latest AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        opened_on_quarter,
        wh_code_ref,
        wh_code_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on_quarter DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
        )
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        MAX(moved_at) AS last_moved_at_2,
        COUNT(*) FILTER (WHERE operator <> 'op_eve') AS move_operator_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.last_moved_at_2,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM screened s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(CASE WHEN operator = 'op_dew' THEN 1 END) AS move_operator_count_2
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count_2, 0) AS move_operator_count_2
    FROM with_stats s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.id
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE capacity > (SELECT MIN(capacity) FROM summarized_rows)
)
SELECT
    id,
    wh_code,
    city,
    capacity,
    opened_on,
    opened_on_quarter,
    last_moved_at_2,
    DATEDIFF(DATE '2025-12-31', last_moved_at) AS days_since_last_moved_at
FROM without_matches
ORDER BY id, opened_on_quarter
