WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    CASE WHEN island_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    BIT_OR(status_bits) AS status_bits_bit_or,
    SUM(status_bits) AS total_status_bits
FROM sessions_device_readings
GROUP BY 1
HAVING total_status_bits > 0
ORDER BY status_bits_bit_or NULLS LAST
