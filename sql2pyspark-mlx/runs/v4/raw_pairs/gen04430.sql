WITH with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    status_bits,
    firmware,
    prev_reading_ts,
    is_new_session,
    island_id,
    CASE WHEN device_code RLIKE '^(DV-04|DV-03)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS device_code_zone
FROM sessionized
ORDER BY status_bits DESC NULLS FIRST;
