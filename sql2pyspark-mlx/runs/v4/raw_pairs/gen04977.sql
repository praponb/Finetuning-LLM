WITH with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_gaps_2 AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
sessionized_device_readings AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps_2
),
gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag_2
    FROM sessionized_device_readings
),
islands AS (
    SELECT *, SUM(new_group_flag_2) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER w_hist island_id_2_cume_dist,
        SUM(island_id) OVER w_running cumulative_island_id,
        NTILE(4) OVER w_part burst_id_bucket,
        NTILE(5) OVER w_part burst_id_bucket_2
    FROM islands
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY island_id_2 DESC), w_running AS (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY device_code ORDER BY burst_id, id)
)
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        island_id_2
    FROM metrics
) src
PIVOT (
    SUM(island_id_2) AS value_sum,
    COUNT(1) AS cnt
    FOR device_code IN ('DV-01' AS dv_01, 'DV-03' AS dv_03)
);
