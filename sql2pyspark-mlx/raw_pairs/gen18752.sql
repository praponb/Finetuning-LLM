WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        MAX(temp_c) OVER (PARTITION BY device_code) AS device_code_max_temp_c,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
        LAG(humidity, 1) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_humidity
    FROM device_readings
    WHERE device_code != 'DV-04'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(temp_c) OVER (ORDER BY device_code_max_temp_c, id) AS next_temp_c,
        LAG(temp_c) OVER (ORDER BY device_code_max_temp_c, id) AS prev_temp_c,
        LAG(running_status_bits) OVER (ORDER BY device_code_max_temp_c, id) AS prev_running_status_bits,
        LEAD(running_status_bits) OVER (ORDER BY device_code_max_temp_c, id) AS next_running_status_bits,
        ROW_NUMBER() OVER (ORDER BY device_code_max_temp_c, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY temp_c, id) AS event_seq
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(prev_humidity) OVER w_hist AS last4_count_prev_humidity,
        MAX(prev_humidity) OVER w_hist AS last4_max_prev_humidity,
        MIN(prev_humidity) OVER w_hist AS last4_min_prev_humidity
    FROM enriched
    WINDOW w_hist AS (ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code_max_temp_c,
        running_status_bits,
        prev_humidity,
        next_temp_c,
        prev_running_status_bits,
        row_seq,
        event_seq,
        last4_count_prev_humidity,
        last4_min_prev_humidity,
        CASE WHEN temp_c >= 36.6 THEN 'High' WHEN temp_c >= 18.8 THEN 'Medium' ELSE 'Low' END AS temp_c_level
    FROM metrics
    WHERE last4_max_prev_humidity >= 76
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        next_temp_c,
        last4_min_prev_humidity
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (next_temp_c, last4_min_prev_humidity)
);
