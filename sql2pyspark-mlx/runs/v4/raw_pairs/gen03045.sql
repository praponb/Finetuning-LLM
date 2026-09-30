with enriched as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        round(temp_c / sum(temp_c) over (partition by device_code), 4) as temp_c_share,
        ntile(5) over (partition by device_code order by temp_c, id) as temp_c_bucket
    from device_readings
)
select
    id,
    device_code,
    reading_ts,
    temp_c,
    status_bits,
    temp_c_share,
    temp_c_bucket,
    case when device_code rlike '^(DV-01|DV-04)$' then 'EXTENDED' when device_code rlike '^(DV-02|DV-03)$' then 'GROUP_B' else 'CORE' end as device_code_zone
from enriched
