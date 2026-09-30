WITH valid AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%l1%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY city), 2) AS capacity_vs_city_avg,
        LAG(capacity_bucket) OVER (PARTITION BY city ORDER BY capacity NULLS LAST, id) AS prev_capacity_bucket,
        NTILE(10) OVER (PARTITION BY city ORDER BY capacity NULLS LAST, id) AS capacity_decile
    FROM valid
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        capacity,
        capacity_bucket,
        capacity_vs_city_avg,
        prev_capacity_bucket,
        capacity_decile,
        ABS(prev_capacity_bucket - 9550) AS prev_capacity_bucket_distance
    FROM metrics
)
-- Final projection
SELECT
    id,
    capacity,
    capacity_vs_city_avg,
    capacity_decile,
    prev_capacity_bucket_distance,
    capacity - capacity_bucket AS capacity_minus_capacity_bucket,
    ROUND(capacity / NULLIF(capacity_bucket, 0), 3) AS capacity_per_capacity_bucket
FROM labeled
ORDER BY id DESC NULLS FIRST, prev_capacity_bucket_distance
LIMIT 20
