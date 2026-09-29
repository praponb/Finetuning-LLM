WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c + status_bits AS temp_c_plus_status_bits
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        firmware
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(pressure, 1) OVER w_running AS prev_pressure,
        SUM(pressure) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure
    FROM latest
    WINDOW w_running AS (ORDER BY pressure, id)
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        pressure,
        firmware,
        prev_pressure,
        running_pressure,
        AVG(running_pressure) OVER (ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_pressure_avg_6_prior,
        CUME_DIST() OVER (ORDER BY pressure DESC NULLS FIRST) AS pressure_cume_dist,
        NTILE(3) OVER (ORDER BY prev_pressure, id) AS prev_pressure_bucket
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        pressure,
        firmware,
        running_pressure_avg_6_prior,
        pressure_cume_dist,
        prev_pressure_bucket,
        NTILE(10) OVER w_part AS prev_pressure_bucket_2,
        SUM(running_pressure_avg_6_prior) OVER w_hist AS running_running_pressure_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY pressure, id) AS row_seq,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM scored_device_readings
    WINDOW w_part AS (ORDER BY prev_pressure, id), w_hist AS (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, prev_pressure_bucket + running_running_pressure_avg_6_prior AS prev_pressure_bucket_plus_running_running_pressure_avg_6_prior
    FROM with_windows
    WHERE humidity BETWEEN 40 AND 44
)
SELECT
    id,
    pressure,
    firmware,
    running_pressure_avg_6_prior,
    pressure_cume_dist,
    prev_pressure_bucket,
    prev_pressure_bucket_2,
    running_running_pressure_avg_6_prior,
    row_seq
FROM cleaned;
