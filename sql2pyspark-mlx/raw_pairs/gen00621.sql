with gaps as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '45' minute then 1 else 0 end as new_group_flag
    from device_readings
),
sessions_device_readings as (
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from gaps
),
scored as (
    select
        *,
        lag(session_no) over (partition by device_code order by new_group_flag, id) as prev_session_no,
        lead(session_no) over (partition by device_code order by new_group_flag, id) as next_session_no,
        lead(humidity) over (partition by device_code order by new_group_flag, id) as next_humidity,
        lag(humidity) over (partition by device_code order by new_group_flag, id) as prev_humidity,
        row_number() over (partition by device_code order by new_group_flag, id) as row_seq,
        cume_dist() over (partition by device_code order by new_group_flag desc nulls first) as new_group_flag_cume_dist
    from sessions_device_readings
),
ranked as (
    select
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        session_no,
        next_session_no,
        next_humidity,
        prev_humidity,
        new_group_flag_cume_dist
    from scored
    qualify row_number() over (partition by device_code order by new_group_flag_cume_dist desc nulls last, id) = 1
),
enriched as (
    select
        *,
        count(next_humidity) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as next_humidity_count_30m,
        first_value(new_group_flag_cume_dist) ignore nulls over (order by new_group_flag_cume_dist, id rows between unbounded preceding and current row) as first_known_new_group_flag_cume_dist
    from ranked
    where new_group_flag_cume_dist <= 0.33
),
gaps_2 as (
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 45 minutes then 1 else 0 end as new_group_flag
    from enriched
),
sessionized as (
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps_2
),
valid as (
    select
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        session_no,
        next_session_no,
        next_humidity,
        prev_humidity,
        new_group_flag_cume_dist,
        first_known_new_group_flag_cume_dist,
        new_group_flag,
        burst_id,
        new_group_flag + pressure as new_group_flag_plus_pressure
    from sessionized
    where next_session_no < 7
),
matched_keys as (
    select firmware, device_code
    from valid
    except
    select firmware, device_code
    from device_readings
    where reading_ts < '2025-03-01'
),
reconciled as (
    select s.*
    from valid s
    inner join matched_keys k on s.firmware = k.firmware and s.device_code = k.device_code
)
select
    date_trunc('hour', reading_ts) as hour_window,
    count(case when device_code = 'DV-02' then 1 end) as dv_02_count,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
    count_if(device_code = 'DV-01') as dv_01_events,
    bit_or(status_bits) as status_bits_bit_or,
    min(first_known_new_group_flag_cume_dist) as min_first_known_new_group_flag_cume_dist
from reconciled
group by date_trunc('hour', reading_ts)
order by hour_window;
