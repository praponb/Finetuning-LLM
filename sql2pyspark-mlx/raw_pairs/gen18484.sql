with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        least(pressure, humidity) as least_pressure_humidity,
        least(status_bits, humidity) as least_status_bits_humidity
    from device_readings
    where status_bits < 10
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        least_status_bits_humidity,
        first_value(pressure, true) over w_grp as first_known_pressure,
        row_number() over (order by temp_c desc, id) as row_seq
    from cleaned
    where firmware = '2.0.1'
    window w_grp as (partition by device_code order by least_pressure_humidity, id rows between unbounded preceding and current row)
),
scored as (
    -- Ranking and gap features
    select
        *,
        avg(least_status_bits_humidity) over (partition by device_code order by pressure, id rows between 3 preceding and current row) as least_status_bits_humidity_avg_3,
        ntile(3) over w_hist as least_status_bits_humidity_bucket,
        ntile(5) over (order by pressure desc, id) as pressure_bucket
    from with_windows
    window w_hist as (order by least_status_bits_humidity, id)
),
cleaned_device_readings as (
    -- Filter the raw rows and derive helper columns
    select *, case when humidity >= 82 then 'High' when humidity >= 41 then 'Medium' else 'Low' end as humidity_band
    from scored
    where least_status_bits_humidity_bucket > 4
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by pressure_bucket desc nulls first, id) as rn
    from cleaned_device_readings
    qualify rn = 1
)
-- Grouped metrics
select
    case when least_status_bits_humidity_bucket >= 9 then 'PREMIUM' else 'STANDARD' end as least_status_bits_humidity_bucket_tier,
    device_code,
    sort_array(collect_set(device_code)) as device_code_list,
    round(avg(least_status_bits_humidity_avg_3), 2) as avg_least_status_bits_humidity_avg_3,
    count_if(device_code = 'DV-02') as dv_02_events
from ranked
group by case when least_status_bits_humidity_bucket >= 9 then 'PREMIUM' else 'STANDARD' end, device_code
order by device_code desc
