WITH valid AS (
    SELECT *, ROUND(status_bits * 1.07, 2) AS adjusted_status_bits
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
      AND firmware NOT IN ('1.0.3', '2.0.1')
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    firmware,
    AVG(status_bits) OVER w_part AS running_avg_status_bits,
    MAX(status_bits) OVER w_part AS running_max_status_bits,
    SUM(status_bits) OVER w_part AS running_sum_status_bits,
    COUNT(status_bits) OVER w_part AS running_count_status_bits,
    ROW_NUMBER() OVER w_hist AS seq_no
FROM valid
WINDOW w_part AS (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY humidity DESC, id)
