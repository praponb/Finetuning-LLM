WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        LAG(humidity, 1, 0) OVER (PARTITION BY device_code ORDER BY status_bits, id) AS prev_humidity,
        MIN(humidity) OVER (PARTITION BY device_code) AS device_code_min_humidity,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity,
        LAST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS temp_c_bucket
    FROM device_readings
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY device_code_min_humidity DESC, id) AS row_seq,
        SUM(temp_c_bucket) OVER w_grp AS cumulative_temp_c_bucket,
        CORR(humidity, temp_c) OVER w_ordered AS corr_humidity_temp_c
    FROM enriched
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY device_code)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_humidity,
        device_code_min_humidity,
        running_humidity,
        last_known_pressure,
        temp_c_bucket,
        row_seq,
        cumulative_temp_c_bucket,
        corr_humidity_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_humidity, id) AS seq_no,
        LEAD(running_humidity, 1) OVER (PARTITION BY device_code ORDER BY corr_humidity_temp_c, id) AS next_running_humidity,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST) AS humidity_dense_rank
    FROM enriched_device_readings
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY seq_no DESC, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY reading_ts DESC) AS dedup_rank
        FROM deduped
    ) ranked
    WHERE dedup_rank = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(cumulative_temp_c_bucket AS DOUBLE) AS cumulative_temp_c_bucket_num
    FROM ranked
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
),
ranked_device_readings AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY dedup_rank DESC, id) AS row_num
        FROM valid
    ) x
    WHERE row_num = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        humidity,
        last_known_pressure,
        pressure,
        corr_humidity_temp_c
    FROM ranked_device_readings
) src
UNPIVOT (
    reading FOR metric IN (humidity AS HUMIDITY, last_known_pressure AS LAST_KNOWN_PRESSURE_VAL, pressure, corr_humidity_temp_c AS CORR_HUMIDITY_TEMP_C)
);
