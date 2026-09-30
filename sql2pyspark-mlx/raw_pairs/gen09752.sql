with base as (
    -- Filter the raw rows and derive helper columns
    select *, cast(status_bits as string) as status_bits_num
    from device_readings
    where reading_ts >= timestamp '2025-09-01 02:00:00'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        substring(firmware, 1, 4) as firmware_prefix
    from base
    where pressure < 994
),
matched_keys as (
    -- Set intersection on keys
    select device_code
    from filtered
    intersect all
    select device_code
    from device_readings
    where device_code in ('DV-04', 'DV-02')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.device_code = k.device_code
),
enriched as (
    -- Ranking and gap features
    select
        *,
        max(temp_c) over (partition by device_code) as device_code_max_temp_c,
        max(pressure) over (partition by date_trunc('week', reading_ts), device_code) as reading_ts_period_max_pressure,
        percent_rank() over (partition by device_code order by temp_c desc) as temp_c_percent_rank
    from kept_rows
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(temp_c_percent_rank) over (partition by date_trunc('WEEK', reading_ts)) as reading_ts_period_count_temp_c_percent_rank,
        count(status_bits) over (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row) as status_bits_count_2h,
        sum(pressure) over w_part as device_code_sum_pressure,
        count(reading_ts_period_max_pressure) over w_part as device_code_count_reading_ts_period_max_pressure,
        cume_dist() over w_running as status_bits_cume_dist
    from enriched
    where temp_c <= 25
    window w_part as (partition by device_code), w_running as (order by status_bits desc)
),
base_device_readings as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), reading_ts) / 365.25 as years_since_reading_ts
    from metrics
    where reading_ts > date '2025-09-01'
      and device_code_count_reading_ts_period_max_pressure between 8 and 15
)
-- Grouped metrics
select
    date_trunc('hour', reading_ts) as hour_window,
    sum(case when device_code = 'DV-03' then humidity else 0 end) as dv_03_humidity,
    count(*) as cnt,
    count(distinct firmware) as distinct_firmwares
from base_device_readings
group by date_trunc('hour', reading_ts);
