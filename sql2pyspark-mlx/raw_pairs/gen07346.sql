with long_format as (
    -- Native SQL UNPIVOT (wide to long)
    select
        id,
        device_code,
        metric,
        reading
    from (
        select
            id,
            device_code,
            temp_c,
            pressure
        from device_readings
    ) src
    unpivot include nulls (
        reading for metric in (temp_c as TEMP_C_VAL, pressure as PRESSURE_VAL)
    )
)
-- Multi-level aggregation
select
    metric,
    sum(reading) as total_reading,
    approx_percentile(reading, array(0.25, 0.5, 0.75)) as reading_quartiles,
    count(*) filter (where metric = 'TEMP_C_VAL') as temp_c_val_rows,
    percentile_approx(reading, 0.9) as reading_p90,
    avg(reading) filter (where metric = 'TEMP_C_VAL') as temp_c_val_avg_reading
from long_format
group by metric
having temp_c_val_avg_reading > 0
