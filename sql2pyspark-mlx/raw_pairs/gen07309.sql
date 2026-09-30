WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, humidity + pressure AS humidity_plus_pressure
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device_code,
        status_bits,
        firmware,
        humidity_plus_pressure
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) AS rn
        FROM ranked
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        status_bits,
        humidity_plus_pressure,
        LAG(status_bits) OVER w_running AS prev_status_bits,
        LAG(humidity_plus_pressure) OVER w_running AS prev_humidity_plus_pressure,
        LEAD(humidity_plus_pressure) OVER w_running AS next_humidity_plus_pressure,
        ROW_NUMBER() OVER w_running AS event_seq,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) AS status_bits_bucket
    FROM deduped
    WHERE rn < 1
    WINDOW w_running AS (PARTITION BY device_code ORDER BY status_bits ASC, id ASC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_bits,
        prev_status_bits,
        next_humidity_plus_pressure,
        AVG(prev_status_bits) OVER (PARTITION BY device_code ORDER BY next_humidity_plus_pressure ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_status_bits_avg_6_prior,
        COUNT(humidity_plus_pressure) OVER w_grp AS device_code_count_humidity_plus_pressure
    FROM enriched
    WINDOW w_grp AS (PARTITION BY device_code)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_bits >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_tier,
    BIT_AND(status_bits) AS status_bits_bit_and,
    ROUND(AVG(device_code_count_humidity_plus_pressure), 2) AS avg_device_code_count_humidity_plus_pressure,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(next_humidity_plus_pressure, ARRAY(0.25, 0.5, 0.75)) AS next_humidity_plus_pressure_quartiles,
    BIT_AND(status_bits) AS status_bits_bit_and_2
FROM with_windows
GROUP BY status_bits_tier
