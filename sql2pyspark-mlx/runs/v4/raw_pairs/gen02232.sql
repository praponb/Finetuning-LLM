with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        concat_ws(',', firmware, device_code) as firmware_device_code_key,
        day(reading_ts) as reading_ts_day
    from device_readings
    where reading_ts >= date '2025-09-01'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 45 minutes then 1 else 0 end as new_group_flag
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from gaps
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by status_bits, id) as event_seq,
        avg(status_bits) over (order by status_bits, id rows between 3 preceding and 1 preceding) as status_bits_avg_3_prior,
        count(temp_c) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as temp_c_count_15m,
        ntile(4) over (partition by device_code order by pressure, id) as pressure_bucket,
        row_number() over (partition by device_code order by reading_ts_day, id) as seq_no
    from sessionized
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        device_code,
        pressure,
        temp_c,
        humidity
    from metrics
) src
unpivot (
    metric_value for measure in (pressure, temp_c, humidity)
);
