WITH cleaned_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        ABS(capacity - 4912) AS capacity_distance,
        ABS(capacity - 11005) AS capacity_distance_2,
        CASE WHEN capacity >= 11506 THEN 'A' WHEN capacity >= 4912 THEN 'B' ELSE 'C' END AS capacity_tier
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at
    FROM cleaned_warehouses s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
deduped AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        capacity_distance,
        capacity_distance_2,
        capacity_tier,
        last_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on DESC, id DESC) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
),
capacity_tier_stats AS (
    -- Max capacity_distance per capacity_tier
    SELECT capacity_tier, MAX(capacity_distance) AS max_capacity_distance
    FROM deduped
    GROUP BY capacity_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_capacity_distance,
        ROUND(s.capacity_distance - g.max_capacity_distance, 2) AS capacity_distance_minus_max
    FROM deduped s
    JOIN capacity_tier_stats g ON g.capacity_tier = s.capacity_tier
    WHERE s.capacity_distance = g.max_capacity_distance
),
city_stats AS (
    -- Median capacity_distance per city
    SELECT city, PERCENTILE_APPROX(capacity_distance, 0.5) AS median_capacity_distance
    FROM above_average
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_capacity_distance,
        ROUND(s.capacity_distance - g.median_capacity_distance, 2) AS capacity_distance_minus_median
    FROM above_average s
    JOIN city_stats g ON g.city = s.city
    WHERE s.capacity_distance >= g.median_capacity_distance
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.sku_id <= 9
        )
      AND s.capacity_distance <= 17000
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    opened_on,
    capacity_distance,
    capacity_distance_2,
    capacity_tier,
    last_moved_at,
    max_capacity_distance,
    capacity_distance_minus_max,
    median_capacity_distance,
    SUBSTRING(TRIM(capacity_tier), 1, 3) AS capacity_tier_prefix
FROM filtered
WHERE capacity_tier IN ('B', 'C')
ORDER BY capacity_distance_2 DESC NULLS FIRST, opened_on
