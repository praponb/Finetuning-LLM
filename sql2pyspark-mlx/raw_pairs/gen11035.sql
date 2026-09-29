with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        case when device_code = 'DV-04' then 1 else 0 end as is_dv_04,
        length(firmware) as firmware_length
    from device_readings
    where reading_ts >= date '2025-09-01'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        max(status_bits) over w_part as running_max_status_bits,
        min(status_bits) over w_part as running_min_status_bits,
        avg(status_bits) over w_part as running_avg_status_bits,
        sum(status_bits) over w_part as running_sum_status_bits,
        avg(is_dv_04) over (partition by device_code) as device_code_avg_is_dv_04
    from valid
    where temp_c between 24 and 31
    window w_part as (partition by device_code order by pressure, id rows between unbounded preceding and current row)
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        firmware,
        is_dv_04,
        firmware_length,
        running_min_status_bits,
        running_avg_status_bits
    from (
        select *, rank() over (partition by device_code order by pressure) as latest_rank
        from scored
    ) t
    where latest_rank = 1
),
valid_device_readings as (
    -- Keep recent, valid rows
    select *, status_bits / nullif(firmware_length, 0) as status_bits_per_firmware_length
    from latest
    where running_avg_status_bits is null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        temp_c,
        pressure,
        status_bits,
        firmware_length,
        running_min_status_bits,
        max(pressure) over (partition by device_code order by running_min_status_bits, id rows between 2 preceding and current row) as last3_max_pressure,
        count(pressure) over (partition by device_code order by running_min_status_bits, id rows between 2 preceding and current row) as last3_count_pressure,
        sum(pressure) over (partition by device_code order by running_min_status_bits, id rows between 2 preceding and current row) as last3_sum_pressure,
        avg(status_bits) over (partition by device_code order by running_avg_status_bits, id rows between 3 preceding and current row) as status_bits_avg_3
    from valid_device_readings
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        temp_c,
        pressure,
        firmware_length,
        running_min_status_bits,
        last3_max_pressure,
        last3_count_pressure,
        last3_sum_pressure,
        status_bits_avg_3,
        ceil(status_bits) as status_bits_whole
    from enriched
    where last3_sum_pressure < 3300
)
-- Window metrics over several different partitions
select
    id,
    temp_c,
    last3_count_pressure,
    last3_sum_pressure,
    count(temp_c) over (order by temp_c, id rows between 6 preceding and current row) as last7_count_temp_c,
    avg(temp_c) over (order by temp_c, id rows between 6 preceding and current row) as last7_avg_temp_c,
    max(temp_c) over (order by temp_c, id rows between 6 preceding and current row) as last7_max_temp_c,
    row_number() over (order by status_bits_avg_3 desc, id) as event_seq
from filtered
