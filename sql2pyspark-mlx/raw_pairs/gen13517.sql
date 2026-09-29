WITH base AS (
    SELECT *, LEAST(humidity, temp_c) AS least_humidity_temp_c
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 04:00:00'
),
cleaned AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        CASE WHEN temp_c >= 33.4 THEN 'High' WHEN temp_c >= 25.7 THEN 'Medium' ELSE 'Low' END AS temp_c_tier
    FROM base
    WHERE reading_ts > TIMESTAMP '2025-09-01 06:00:00'
),
scored AS (
    SELECT
        *,
        LEAD(status_bits) OVER (PARTITION BY device_code ORDER BY pressure, id) AS next_status_bits,
        LAG(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_humidity,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS event_seq_2
    FROM cleaned
),
with_windows AS (
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        temp_c_tier,
        event_seq_2,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_bits,
        COUNT(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_status_bits,
        MIN(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_bits
    FROM scored
    WHERE event_seq > 4
),
cleaned_device_readings AS (
    SELECT *, running_count_status_bits + pressure AS running_count_status_bits_plus_pressure
    FROM with_windows
    WHERE status_bits < 4
),
with_windows_device_readings AS (
    SELECT
        id,
        temp_c_tier,
        running_count_status_bits,
        running_count_status_bits_plus_pressure,
        AVG(event_seq_2) OVER w_recent AS last7_avg_event_seq_2,
        COUNT(event_seq_2) OVER w_recent AS last7_count_event_seq_2,
        SUM(event_seq_2) OVER w_recent AS last7_sum_event_seq_2
    FROM cleaned_device_readings
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY running_count_status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored_device_readings AS (
    SELECT
        id,
        running_count_status_bits,
        last7_avg_event_seq_2,
        MAX(last7_sum_event_seq_2) OVER w_grp AS running_max_last7_sum_event_seq_2,
        AVG(last7_sum_event_seq_2) OVER w_grp AS running_avg_last7_sum_event_seq_2,
        SUM(last7_sum_event_seq_2) OVER w_grp AS running_sum_last7_sum_event_seq_2,
        SUM(last7_count_event_seq_2) OVER (ORDER BY last7_avg_event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_count_event_seq_2
    FROM with_windows_device_readings
    WINDOW w_grp AS (ORDER BY running_count_status_bits_plus_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN cumulative_last7_count_event_seq_2 >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_last7_count_event_seq_2_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(running_avg_last7_sum_event_seq_2, 0.9) AS running_avg_last7_sum_event_seq_2_p90,
    APPROX_PERCENTILE(running_max_last7_sum_event_seq_2, ARRAY(0.25, 0.5, 0.75)) AS running_max_last7_sum_event_seq_2_quartiles,
    APPROX_PERCENTILE(last7_avg_event_seq_2, 0.95) AS last7_avg_event_seq_2_p95
FROM scored_device_readings
GROUP BY CASE WHEN cumulative_last7_count_event_seq_2 >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING last7_avg_event_seq_2_p95 >= 0
ORDER BY last7_avg_event_seq_2_p95 DESC NULLS LAST;
