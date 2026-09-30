with cleaned as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        to_date(reading_ts) as reading_ts_date,
        round(pressure, 1) as pressure_rounded
    from device_readings
    where reading_ts >= timestamp '2025-09-01 06:00:00'
),
flagged as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 1 hour then 1 else 0 end as session_start_flag
    from cleaned
),
sessions_device_readings as (
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from flagged
)
select
    session_no,
    min(reading_ts) as first_seen,
    sort_array(collect_set(device_code)) as device_code_list,
    count(distinct id) as distinct_ids,
    sum(pressure_rounded) filter (where device_code = 'DV-01') as dv_01_total_pressure_rounded,
    count(*) filter (where device_code = 'DV-03') as dv_03_rows,
    count(case when device_code = 'DV-02' then 1 end) as dv_02_count
from sessions_device_readings
group by session_no
order by dv_03_rows nulls first;
