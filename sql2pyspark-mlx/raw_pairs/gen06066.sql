WITH matched_keys AS (
    -- Set intersection on keys
    SELECT firmware, id
    FROM device_readings
    INTERSECT ALL
    SELECT firmware, id
    FROM device_readings
    WHERE reading_ts < '2024-11-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN matched_keys k ON s.firmware = k.firmware AND s.id = k.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        COUNT(temp_c) OVER w_hist AS temp_c_count_30m,
        PERCENT_RANK() OVER (ORDER BY humidity DESC NULLS LAST) AS humidity_percent_rank
    FROM reconciled
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(firmware)) AS firmware_title
    FROM metrics
    WHERE temp_c > 27
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT
    SELECT id
    FROM device_readings
    WHERE temp_c < 25
),
reconciled_device_readings AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Running and moving aggregates
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY status_bits DESC) AS status_bits_percent_rank,
    MAX(temp_c_count_30m) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS temp_c_count_30m_max_30m,
    COUNT(humidity_percent_rank) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS humidity_percent_rank_count_1h
FROM reconciled_device_readings
