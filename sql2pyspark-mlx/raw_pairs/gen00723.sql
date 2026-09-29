WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c NULLS LAST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        reading_ts,
        temp_c
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST, id) AS rn
        FROM ranked
    ) t
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(temp_c, 1) OVER (ORDER BY temp_c, id) AS next_temp_c,
        LAST_VALUE(temp_c, TRUE) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c
    FROM deduped
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id
    FROM device_readings
    WHERE status_bits <= 5
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        last_known_temp_c,
        SUM(last_known_temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS last_known_temp_c_sum_1h
    FROM verified
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(last_known_temp_c_sum_1h * 100, 2) AS last_known_temp_c_sum_1h_scaled,
        CASE WHEN last_known_temp_c_sum_1h >= 155 THEN 'High' WHEN last_known_temp_c_sum_1h >= 46 THEN 'Medium' ELSE 'Low' END AS last_known_temp_c_sum_1h_band
    FROM scored
    WHERE last_known_temp_c_sum_1h > 86
)
SELECT
    id,
    reading_ts,
    temp_c,
    last_known_temp_c
FROM valid
ORDER BY id
