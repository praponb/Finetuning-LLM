WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN humidity >= 64 THEN 'High' WHEN humidity >= 48 THEN 'Medium' ELSE 'Low' END AS humidity_tier,
        humidity + temp_c AS humidity_plus_temp_c
    FROM device_readings
    WHERE pressure BETWEEN 995 AND 996
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        firmware,
        humidity_tier,
        humidity_plus_temp_c,
        is_new_burst
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_plus_temp_c DESC, id) AS dedup_rank
        FROM islands
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        firmware,
        humidity_tier,
        is_new_burst,
        LAG(humidity_plus_temp_c, 2) OVER (PARTITION BY device_code ORDER BY humidity, id) AS prev_humidity_plus_temp_c,
        LEAD(is_new_burst, 1) OVER (ORDER BY pressure, id) AS next_is_new_burst
    FROM latest
),
latest_device_readings AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST, id) = 1
)
-- Grouped metrics
SELECT
    device_code,
    CASE WHEN prev_humidity_plus_temp_c >= 69 THEN 'HIGH' ELSE 'LOW' END AS prev_humidity_plus_temp_c_tier,
    STDDEV_SAMP(humidity) AS humidity_stddev,
    PERCENTILE_APPROX(next_is_new_burst, ARRAY(0.25, 0.5, 0.75)) AS next_is_new_burst_quartiles,
    AVG(prev_humidity_plus_temp_c) AS avg_prev_humidity_plus_temp_c,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    ROUND(SUM(humidity) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS humidity_per_dv_04,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events
FROM latest_device_readings
GROUP BY 1, 2
HAVING AVG(prev_humidity_plus_temp_c) > 0
ORDER BY humidity_stddev NULLS LAST;
