WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    burst_id,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS is_new_burst_per_dv_01,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    APPROX_PERCENTILE(is_new_burst, ARRAY(0.25, 0.5, 0.75)) AS is_new_burst_quartiles,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    MAX(pressure) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_pressure
FROM sessions_device_readings
GROUP BY burst_id
ORDER BY dv_01_rows NULLS LAST;
