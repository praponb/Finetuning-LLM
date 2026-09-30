SELECT
    *,
    LAG(status_bits) OVER w_part AS prev_status_bits,
    LEAD(status_bits) OVER w_part AS next_status_bits,
    LEAD(pressure) OVER w_part AS next_pressure,
    LAG(pressure) OVER w_part AS prev_pressure,
    ROW_NUMBER() OVER w_part AS event_seq
FROM device_readings
WINDOW w_part AS (PARTITION BY device_code ORDER BY status_bits, id);
