WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(status_bits * 100, 2) AS status_bits_adjusted
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 02:00:00'
      AND humidity < 38
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    BIT_AND(status_bits) AS status_bits_bit_and,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    MIN(reading_ts) AS first_seen
FROM valid
GROUP BY DATE_TRUNC('HOUR', reading_ts)
