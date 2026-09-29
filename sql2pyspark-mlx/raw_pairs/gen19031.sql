WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        island_id,
        AVG(temp_c) OVER w_grp AS temp_c_avg_6_prior,
        MIN(humidity) OVER (PARTITION BY device_code) AS device_code_min_humidity,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
        FIRST_VALUE(humidity) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', reading_ts), device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_humidity
    FROM islands
    WHERE temp_c >= 27
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        temp_c,
        humidity,
        status_bits,
        firmware,
        device_code_min_humidity,
        humidity_share,
        first_known_humidity,
        SUBSTRING(device_code, 1, 3) AS device_code_prefix
    FROM scored
    WHERE first_known_humidity >= 65
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, firmware
    FROM base
    INTERSECT ALL
    SELECT id, firmware
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 6 AND 11
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id AND s.firmware = k.firmware
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN humidity_share >= 0.69 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_share_tier,
    SUM(status_bits) AS total_status_bits,
    COUNT(*) AS num_rows,
    BIT_AND(status_bits) AS status_bits_bit_and
FROM kept_rows
GROUP BY CASE WHEN humidity_share >= 0.69 THEN 'PREMIUM' ELSE 'STANDARD' END
