WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS rn
        FROM device_readings
    ) x
    WHERE rn = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
islands_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, island_id * status_bits AS island_id_times_status_bits
    FROM islands_device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 09:00:00'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(status_bits) OVER w_running AS last7_count_status_bits,
        SUM(status_bits) OVER w_running AS last7_sum_status_bits,
        MIN(status_bits) OVER w_running AS last7_min_status_bits,
        LAST_VALUE(island_id_times_status_bits) IGNORE NULLS OVER w_ordered AS last_known_island_id_times_status_bits
    FROM base
    WHERE status_bits > 10
    WINDOW w_running AS (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY device_code ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT device_code
    FROM scored
    INTERSECT
    SELECT device_code
    FROM device_readings
    WHERE pressure <= 1021
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.device_code = k.device_code
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id_times_status_bits >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_times_status_bits_tier,
    BIT_OR(status_bits) AS status_bits_bit_or,
    COUNT(*) AS row_count
FROM verified
GROUP BY ROLLUP (CASE WHEN island_id_times_status_bits >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END);
