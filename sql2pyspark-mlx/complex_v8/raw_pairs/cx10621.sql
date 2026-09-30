WITH base_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        DATE_TRUNC('WEEK', opened_on) AS opened_on_week,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        LOWER(TRIM(city)) AS city_clean
    FROM warehouses
    WHERE (city = 'Kuala Lumpur' OR capacity > 11005)
),
city_stats AS (
    -- Max capacity per city
    SELECT city, MAX(capacity) AS max_capacity
    FROM base_warehouses
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_capacity,
        ROUND(s.capacity - g.max_capacity, 2) AS capacity_minus_max
    FROM base_warehouses s
    JOIN city_stats g ON g.city = s.city
    WHERE s.capacity = g.max_capacity
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        wh_code,
        city,
        opened_on_week,
        capacity_bucket,
        max_capacity,
        capacity_minus_max,
        ROUND(PERCENT_RANK() OVER (PARTITION BY city ORDER BY max_capacity, id), 3) AS max_capacity_percent_rank,
        capacity - LAG(capacity) OVER (PARTITION BY city ORDER BY max_capacity, id) AS capacity_change,
        ROUND(capacity_bucket / NULLIF(SUM(capacity_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on_week), city), 0), 4) AS capacity_bucket_share_of_opened_on_week_month_ci
    FROM outliers
)
SELECT
    id,
    wh_code,
    opened_on_week,
    capacity_bucket,
    capacity_minus_max,
    max_capacity_percent_rank,
    capacity_change,
    COALESCE(city, 'UNKNOWN') AS city_or_unknown,
    LEAST(capacity_bucket, capacity_minus_max) AS least_capacity_bucket_capacity_minus_max
FROM windowed
ORDER BY opened_on_week
