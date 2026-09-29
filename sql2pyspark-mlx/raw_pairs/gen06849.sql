with valid as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        upper(device_code) as device_code_upper
    from device_readings
    where firmware not in ('1.1.0', '2.0.1')
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '20' minute then 1 else 0 end as is_new_session
    from valid
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '45' minute then 1 else 0 end as new_group_flag
    from sessionized
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from gaps
),
kept_keys as (
    -- Set intersection on keys
    select firmware, device_code
    from sessions_device_readings
    intersect all
    select firmware, device_code
    from device_readings
    where device_code <> 'DV-02'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_device_readings s
    join kept_keys k on s.firmware = k.firmware and s.device_code = k.device_code
),
valid_device_readings as (
    -- Keep recent, valid rows
    select *, greatest(new_group_flag, session_id) as greatest_new_group_flag_session_id
    from kept_rows
    where reading_ts >= '2025-02-01'
),
valid_device_readings_2 as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        device_code_upper,
        is_new_session,
        greatest_new_group_flag_session_id,
        day(reading_ts) as reading_ts_day,
        floor(session_no) as session_no_whole
    from valid_device_readings
    where reading_ts >= date '2025-09-01'
      and status_bits = 11
),
base as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        status_bits,
        device_code_upper,
        is_new_session,
        greatest_new_group_flag_session_id,
        reading_ts_day,
        session_no_whole,
        upper(device_code) as device_code_upper_2
    from valid_device_readings_2
    where reading_ts > timestamp '2025-09-01 07:00:00'
)
select
    id,
    reading_ts,
    device_code_upper,
    is_new_session,
    greatest_new_group_flag_session_id,
    session_no_whole,
    device_code_upper_2
from base
order by greatest_new_group_flag_session_id desc nulls first;
