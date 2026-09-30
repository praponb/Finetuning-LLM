select
    device_code,
    case when temp_c >= 39 then 'LARGE' else 'SMALL' end as temp_c_tier,
    date_trunc('day', reading_ts) as activity_day,
    count(*) filter (where device_code = 'DV-04') as dv_04_rows,
    count_if(device_code = 'DV-03') as dv_03_events,
    sum(case when device_code = 'DV-02' then pressure else 0 end) as dv_02_pressure
from device_readings
group by device_code, case when temp_c >= 39 then 'LARGE' else 'SMALL' end, date_trunc('day', reading_ts)
order by dv_03_events;
