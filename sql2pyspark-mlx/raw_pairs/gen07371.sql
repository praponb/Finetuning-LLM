WITH metrics AS (
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        AVG(temp_c) OVER w_ordered AS running_avg_temp_c,
        MIN(temp_c) OVER w_ordered AS running_min_temp_c,
        SUM(temp_c) OVER w_ordered AS running_sum_temp_c,
        LAG(humidity, 1) OVER w_hist AS prev_humidity
    FROM device_readings
    WHERE status_bits < 4
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY device_code ORDER BY humidity, id)
),
ranked AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
deduped AS (
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY reading_ts) = 1
),
with_windows AS (
    SELECT
        id,
        prev_humidity,
        SUM(running_avg_temp_c) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_avg_temp_c,
        SUM(status_bits) OVER (PARTITION BY YEAR(reading_ts)) AS reading_ts_year_sum_status_bits,
        NTILE(3) OVER (ORDER BY running_avg_temp_c DESC, id) AS running_avg_temp_c_bucket
    FROM deduped
),
scored AS (
    SELECT
        id,
        prev_humidity,
        cumulative_running_avg_temp_c,
        running_avg_temp_c_bucket,
        ROW_NUMBER() OVER (ORDER BY prev_humidity DESC, id) AS event_seq,
        LAST_VALUE(prev_humidity) IGNORE NULLS OVER w_running AS last_known_prev_humidity,
        SUM(reading_ts_year_sum_status_bits) OVER w_hist AS cumulative_reading_ts_year_sum_status_bits,
        LAST_VALUE(reading_ts_year_sum_status_bits) IGNORE NULLS OVER w_running AS last_known_reading_ts_year_sum_status_bits,
        LAST_VALUE(cumulative_running_avg_temp_c) IGNORE NULLS OVER (ORDER BY prev_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_running_avg_temp_c
    FROM with_windows
    WINDOW w_running AS (ORDER BY running_avg_temp_c_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY cumulative_running_avg_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        *,
        FLOOR(last_known_cumulative_running_avg_temp_c) AS last_known_cumulative_running_avg_temp_c_whole,
        FLOOR(last_known_cumulative_running_avg_temp_c) AS last_known_cumulative_running_avg_temp_c_whole_2
    FROM scored
    WHERE last_known_prev_humidity > 42
),
metrics_device_readings AS (
    SELECT
        *,
        LEAD(last_known_prev_humidity) OVER w_running AS next_last_known_prev_humidity,
        LEAD(event_seq) OVER w_running AS next_event_seq,
        LAG(event_seq) OVER w_running AS prev_event_seq,
        ROW_NUMBER() OVER w_running AS event_seq_2,
        PERCENT_RANK() OVER (ORDER BY event_seq DESC NULLS FIRST) AS event_seq_percent_rank
    FROM base
    WINDOW w_running AS (ORDER BY last_known_reading_ts_year_sum_status_bits, id)
),
report AS (
    SELECT
        id,
        prev_humidity,
        running_avg_temp_c_bucket,
        event_seq,
        cumulative_reading_ts_year_sum_status_bits,
        last_known_reading_ts_year_sum_status_bits,
        last_known_cumulative_running_avg_temp_c,
        last_known_cumulative_running_avg_temp_c_whole,
        next_last_known_prev_humidity
    FROM metrics_device_readings
    ORDER BY prev_humidity DESC NULLS FIRST
)
SELECT
    id,
    prev_humidity,
    running_avg_temp_c_bucket,
    event_seq,
    last_known_reading_ts_year_sum_status_bits,
    last_known_cumulative_running_avg_temp_c,
    last_known_cumulative_running_avg_temp_c_whole
FROM report
ORDER BY last_known_reading_ts_year_sum_status_bits DESC, last_known_cumulative_running_avg_temp_c_whole;
