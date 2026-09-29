WITH latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST) AS row_num
        FROM device_readings
    ) ranked
    WHERE row_num = 1
),
gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM latest
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
sessions_device_readings AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
gaps_2 AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag_2
    FROM sessions_device_readings
),
islands_device_readings AS (
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps_2
),
enriched AS (
    SELECT
        *,
        MAX(status_bits) OVER (PARTITION BY device_code) AS device_code_max_status_bits,
        ROW_NUMBER() OVER (ORDER BY session_id ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_id DESC, id ASC) AS event_seq,
        FIRST_VALUE(session_start_flag) IGNORE NULLS OVER (ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start_flag
    FROM islands_device_readings
),
latest_device_readings AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_start_flag DESC, id ASC) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    SELECT
        *,
        AVG(device_code_max_status_bits) OVER (PARTITION BY device_code ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS device_code_max_status_bits_avg_5,
        ROW_NUMBER() OVER (ORDER BY dedup_rank ASC, id ASC) AS event_seq_2,
        SUM(island_id_2) OVER (PARTITION BY device_code ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_island_id_2
    FROM latest_device_readings
)
SELECT
    id,
    device_code,
    humidity,
    status_bits,
    firmware,
    row_num,
    is_new_burst,
    session_start_flag,
    island_id,
    CASE WHEN device_code RLIKE '^(DV-01|DV-03)$' THEN 'CORE' ELSE 'GROUP_B' END AS device_code_zone
FROM with_windows
ORDER BY session_start_flag DESC NULLS LAST, firmware DESC;
