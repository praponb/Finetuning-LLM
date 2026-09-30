WITH filtered AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c - pressure temp_c_minus_pressure
    FROM device_readings
    WHERE pressure BETWEEN 1009 AND 1023
),
filtered_device_readings AS (
    SELECT *, CEIL(status_bits) status_bits_whole
    FROM filtered
    WHERE status_bits BETWEEN 8 AND 10
),
deduped AS (
    SELECT *
    FROM filtered_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_minus_pressure, id) = 1
),
scored AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        firmware,
        temp_c_minus_pressure,
        LAG(status_bits_whole) OVER w_ordered prev_status_bits_whole,
        LEAD(status_bits_whole) OVER w_ordered next_status_bits_whole,
        LAG(temp_c_minus_pressure) OVER w_ordered prev_temp_c_minus_pressure,
        LEAD(temp_c_minus_pressure) OVER w_ordered next_temp_c_minus_pressure,
        ROW_NUMBER() OVER w_ordered seq_no,
        DENSE_RANK() OVER w_hist humidity_dense_rank
    FROM deduped
    WHERE device_code = 'DV-01'
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY pressure, id), w_hist AS (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST)
),
base AS (
    SELECT *, INITCAP(TRIM(device_code)) device_code_title
    FROM scored
    WHERE next_temp_c_minus_pressure BETWEEN 51 AND 58
),
latest AS (
    SELECT
        id,
        temp_c,
        humidity,
        temp_c_minus_pressure,
        prev_status_bits_whole,
        next_status_bits_whole,
        prev_temp_c_minus_pressure,
        next_temp_c_minus_pressure,
        seq_no,
        humidity_dense_rank,
        device_code_title
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity NULLS FIRST, id) = 1
),
scored_device_readings AS (
    SELECT
        *,
        MIN(seq_no) OVER w_grp last3_min_seq_no,
        MAX(seq_no) OVER w_grp last3_max_seq_no,
        SUM(seq_no) OVER w_grp last3_sum_seq_no,
        AVG(prev_status_bits_whole) OVER (ORDER BY temp_c, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) prev_status_bits_whole_avg_6_prior
    FROM latest
    WINDOW w_grp AS (ORDER BY next_status_bits_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    humidity,
    temp_c_minus_pressure,
    prev_status_bits_whole,
    next_status_bits_whole,
    prev_temp_c_minus_pressure,
    next_temp_c_minus_pressure,
    humidity_dense_rank,
    device_code_title
FROM scored_device_readings;
