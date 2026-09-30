WITH scored AS (
    SELECT
        id,
        device_code,
        humidity,
        firmware,
        LEAD(temp_c) OVER w_recent AS next_temp_c,
        LAG(temp_c) OVER w_recent AS prev_temp_c,
        LAG(pressure) OVER w_recent AS prev_pressure,
        LEAD(pressure) OVER w_recent AS next_pressure,
        ROW_NUMBER() OVER w_recent AS event_seq,
        AVG(status_bits) OVER (ORDER BY temp_c, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_bits_avg_2
    FROM device_readings
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY status_bits, id)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_avg_2 DESC NULLS LAST, id) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
),
valid AS (
    SELECT
        id,
        device_code,
        humidity,
        firmware,
        next_temp_c,
        prev_pressure,
        next_pressure,
        event_seq,
        row_num,
        prev_temp_c - humidity AS prev_temp_c_minus_humidity,
        CAST(next_pressure AS INT) AS next_pressure_int
    FROM deduped
    WHERE firmware = '1.1.0'
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY prev_temp_c_minus_humidity DESC, id) AS prev_temp_c_minus_humidity_bucket,
        DENSE_RANK() OVER w_hist AS event_seq_dense_rank
    FROM valid
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY event_seq NULLS FIRST)
)
SELECT
    id,
    device_code,
    metric_name,
    val
FROM (
    SELECT
        id,
        device_code,
        prev_pressure,
        next_pressure
    FROM with_windows
) src
UNPIVOT (
    val FOR metric_name IN (prev_pressure, next_pressure)
)
