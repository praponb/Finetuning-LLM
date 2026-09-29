SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC) AS event_seq,
    DENSE_RANK() OVER (PARTITION BY device_code ORDER BY pressure ASC NULLS FIRST) AS pressure_dense_rank,
    CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity
FROM device_readings;
