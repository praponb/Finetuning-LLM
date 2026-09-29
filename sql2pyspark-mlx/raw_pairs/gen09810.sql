WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(temp_c * 0.85, 2) AS temp_c_adjusted
    FROM device_readings
    WHERE device_code NOT IN ('DV-02', 'DV-03', 'DV-04')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    device_code,
    CASE WHEN session_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_tier,
    CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM sessionized
GROUP BY ROLLUP (device_code, CASE WHEN session_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END)
HAVING event_count > 1;
