SELECT
    id,
    device_code,
    reading_ts,
    status_bits,
    MAX(temp_c) OVER (PARTITION BY device_code) device_code_max_temp_c,
    AVG(temp_c) OVER w_hist temp_c_avg_2_prior,
    ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) humidity_share,
    RANK() OVER (PARTITION BY device_code ORDER BY status_bits) status_bits_rank,
    NTILE(5) OVER (ORDER BY pressure DESC, id) pressure_bucket
FROM device_readings
WHERE firmware != '1.1.0'
WINDOW w_hist AS (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
