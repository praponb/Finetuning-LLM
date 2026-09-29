with remaining_keys as (
    select device_code, id
    from device_readings
    intersect
    select device_code, id
    from device_readings
    where pressure < 1028
),
verified as (
    select s.*
    from device_readings s
    join remaining_keys k on s.device_code = k.device_code and s.id = k.id
)
select
    case when pressure >= 1028 then 'HIGH' else 'LOW' end as pressure_tier,
    date_trunc('hour', reading_ts) as hour_window,
    approx_percentile(pressure, array(0.25, 0.5, 0.75)) as pressure_quartiles,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as duration_min,
    count(case when device_code = 'DV-04' then 1 end) as dv_04_count,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as duration_min_2,
    grouping(case when pressure >= 1028 then 'HIGH' else 'LOW' end) as grouping_level
from verified
group by rollup (case when pressure >= 1028 then 'HIGH' else 'LOW' end, date_trunc('hour', reading_ts))
