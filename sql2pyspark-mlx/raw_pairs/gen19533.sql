WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CASE WHEN firmware = '1.0.3' THEN 1 ELSE 0 END AS is_1_0_3,
        CASE WHEN firmware = '1.1.0' THEN 1 ELSE 0 END AS is_1_1_0
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND status_bits < 11
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        firmware,
        is_1_0_3,
        is_1_1_0,
        SUM(is_1_1_0) OVER (PARTITION BY device_code) AS device_code_sum_is_1_1_0,
        LAG(is_1_1_0, 1) OVER (ORDER BY is_1_0_3, id) AS prev_is_1_1_0
    FROM filtered
    WHERE temp_c BETWEEN 31 AND 35
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(prev_is_1_1_0) OVER (PARTITION BY device_code ORDER BY device_code_sum_is_1_1_0, id) AS next_prev_is_1_1_0,
        LAG(prev_is_1_1_0) OVER (PARTITION BY device_code ORDER BY device_code_sum_is_1_1_0, id) AS prev_prev_is_1_1_0,
        LEAD(is_1_0_3) OVER (PARTITION BY device_code ORDER BY device_code_sum_is_1_1_0, id) AS next_is_1_0_3,
        LAG(is_1_0_3) OVER (PARTITION BY device_code ORDER BY device_code_sum_is_1_1_0, id) AS prev_is_1_0_3,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY device_code_sum_is_1_1_0, id) AS seq_no,
        LAST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY prev_is_1_1_0, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure
    FROM with_windows
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        pressure,
        firmware,
        is_1_1_0,
        device_code_sum_is_1_1_0,
        prev_prev_is_1_1_0,
        next_is_1_0_3,
        prev_is_1_0_3,
        seq_no,
        last_known_pressure,
        LEAD(next_prev_is_1_1_0) OVER (PARTITION BY device_code ORDER BY prev_is_1_1_0, id) AS next_next_prev_is_1_1_0,
        LAG(next_is_1_0_3) OVER (PARTITION BY device_code ORDER BY prev_is_1_1_0, id) AS prev_next_is_1_0_3,
        LEAD(next_is_1_0_3) OVER (PARTITION BY device_code ORDER BY prev_is_1_1_0, id) AS next_next_is_1_0_3,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_is_1_1_0, id) AS seq_no_2,
        AVG(next_is_1_0_3) OVER (PARTITION BY device_code ORDER BY prev_prev_is_1_1_0, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_is_1_0_3_avg_5
    FROM enriched
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY seq_no_2, id) AS event_seq,
        LAG(next_next_is_1_0_3, 1) OVER (ORDER BY device_code_sum_is_1_1_0, id) AS prev_next_next_is_1_0_3,
        ROUND(seq_no_2 / SUM(seq_no_2) OVER (PARTITION BY device_code), 4) AS seq_no_2_share,
        NTILE(4) OVER (ORDER BY prev_prev_is_1_1_0, id) AS prev_prev_is_1_1_0_bucket
    FROM metrics
),
daily AS (
    -- Multi-level aggregation
    SELECT
        seq_no,
        COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
        MAX(prev_prev_is_1_1_0_bucket) AS max_prev_prev_is_1_1_0_bucket,
        ROUND(SUM(prev_prev_is_1_1_0) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS prev_prev_is_1_1_0_per_dv_01,
        COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count
    FROM with_windows_device_readings
    GROUP BY seq_no
    HAVING max_prev_prev_is_1_1_0_bucket > 0
)
SELECT
    seq_no,
    dv_03_count,
    max_prev_prev_is_1_1_0_bucket,
    prev_prev_is_1_1_0_per_dv_01,
    dv_02_count
FROM daily
ORDER BY prev_prev_is_1_1_0_per_dv_01 DESC
