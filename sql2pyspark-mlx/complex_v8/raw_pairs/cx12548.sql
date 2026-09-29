WITH filtered AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city IS NOT NULL
      AND opened_on >= DATE '2024-04-13'
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        SUM(unit_price) AS total_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.total_move_unit_price
    FROM filtered s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug,
        CONCAT(city, '#', CAST(move_count AS STRING)) AS city_ref,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM with_stats
)
SELECT id, field, value
FROM (
    SELECT id, capacity, move_count
    FROM labeled
) src
UNPIVOT (
    value FOR field IN (capacity, move_count)
) unp
