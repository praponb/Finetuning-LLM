select
    id,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    firmware,
    lead(status_bits) over (partition by device_code order by pressure, id) as next_status_bits,
    lag(pressure) over (partition by device_code order by pressure, id) as prev_pressure,
    lead(pressure) over (partition by device_code order by pressure, id) as next_pressure,
    row_number() over (partition by device_code order by pressure, id) as seq_no,
    ntile(5) over (order by pressure, id) as pressure_bucket
from device_readings;
