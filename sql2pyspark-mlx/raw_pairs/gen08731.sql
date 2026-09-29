WITH with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        humidity / NULLIF(status_bits, 0) AS humidity_per_status_bits
    FROM sessions_device_readings
    WHERE island_id = 4
),
scored AS (
    SELECT
        id,
        device_code,
        firmware,
        humidity_per_status_bits,
        ROW_NUMBER() OVER (PARTITION BY YEAR(reading_ts), device_code ORDER BY humidity_per_status_bits, id) AS event_seq,
        COUNT(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pressure_count_30m
    FROM valid
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY event_seq DESC, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
valid_device_readings AS (
    SELECT
        id,
        humidity_per_status_bits,
        event_seq,
        row_num,
        LEAST(pressure_count_30m, event_seq) AS least_pressure_count_30m_event_seq
    FROM ranked
    WHERE firmware IN ('1.1.0', '2.0.1', '1.0.3')
)
SELECT
    id,
    least_pressure_count_30m_event_seq,
    LAG(event_seq, 1) OVER (ORDER BY event_seq, id) AS prev_event_seq,
    SUM(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq,
    NTILE(3) OVER (ORDER BY event_seq DESC, id) AS event_seq_bucket
FROM valid_device_readings;
