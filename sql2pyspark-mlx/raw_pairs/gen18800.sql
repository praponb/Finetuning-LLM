with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(status_bits as double) as status_bits_num,
        cast(reading_ts as date) as reading_ts_day
    from device_readings
    where temp_c is null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) + interval '30' minute then 1 else 0 end as session_start_flag
    from valid
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        status_bits_num,
        sum(session_start_flag) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
),
scored as (
    -- Running and moving aggregates
    select
        *,
        min(island_id) over (partition by device_code order by status_bits asc, id asc rows between unbounded preceding and current row) as running_min_island_id,
        count(island_id) over (partition by device_code order by status_bits asc, id asc rows between unbounded preceding and current row) as running_count_island_id,
        sum(island_id) over (partition by device_code order by status_bits asc, id asc rows between unbounded preceding and current row) as running_sum_island_id
    from sessions_device_readings
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by device_code order by temp_c desc nulls first) as row_num
        from scored
    ) x
    where row_num = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(status_bits) over (partition by device_code order by running_count_island_id asc, id asc rows between 5 preceding and 1 preceding) as status_bits_avg_5_prior,
        rank() over (partition by device_code order by running_sum_island_id asc nulls last) as running_sum_island_id_rank
    from latest
    where island_id > 3
),
cleaned as (
    -- Basic cleansing and defaults
    select *, round(status_bits_avg_5_prior * 0.9, 2) as status_bits_avg_5_prior_scaled
    from metrics
    where reading_ts >= date '2025-09-01'
),
metrics_device_readings as (
    -- Window metrics over several different partitions
    select
        *,
        min(running_sum_island_id_rank) over (partition by device_code) as device_code_min_running_sum_island_id_rank,
        avg(temp_c) over (partition by device_code) as device_code_avg_temp_c,
        lead(status_bits_avg_5_prior, 1) over (partition by device_code order by row_num asc, id asc) as next_status_bits_avg_5_prior
    from cleaned
)
-- Grouped metrics
select
    case when temp_c >= 38.2 then 'PREMIUM' else 'STANDARD' end as temp_c_tier,
    bit_and(status_bits) as status_bits_bit_and,
    count(case when device_code = 'DV-04' then 1 end) as dv_04_count,
    count(*) filter (where device_code = 'DV-04') as dv_04_rows,
    round(sum(status_bits) / nullif(count(case when device_code = 'DV-03' then 1 end), 0), 2) as status_bits_per_dv_03,
    approx_percentile(device_code_avg_temp_c, array(0.25, 0.5, 0.75)) as device_code_avg_temp_c_quartiles,
    size(collect_set(device_code)) as num_device_codes
from metrics_device_readings
group by case when temp_c >= 38.2 then 'PREMIUM' else 'STANDARD' end
