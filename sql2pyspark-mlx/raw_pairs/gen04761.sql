with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        cast(status_bits as double) as status_bits_num
    from device_readings
    where device_code = 'DV-04'
),
kept_keys as (
    -- Set difference on keys
    select id, firmware
    from filtered
    except all
    select id, firmware
    from device_readings
    where firmware <> '1.1.0'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join kept_keys k on s.id = k.id and s.firmware = k.firmware
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        datediff(current_date(), reading_ts) as days_since_reading_ts,
        round(pressure * 1.1, 2) as pressure_scaled
    from kept_rows
    where reading_ts >= date '2025-09-01'
),
filtered_device_readings as (
    -- Basic cleansing and defaults
    select
        id,
        temp_c,
        humidity,
        pressure,
        firmware,
        days_since_reading_ts,
        pressure_scaled,
        round(pressure_scaled, 0) as pressure_scaled_rounded
    from valid
    where reading_ts >= date '2025-09-01'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        pressure,
        firmware,
        pressure_scaled,
        pressure_scaled_rounded,
        least(pressure_scaled_rounded, pressure) as least_pressure_scaled_rounded_pressure
    from filtered_device_readings
    where pressure_scaled_rounded between 40 and 64
)
-- Aggregate with conditional metrics
select
    case when pressure_scaled >= 66 then 'LARGE' else 'SMALL' end as pressure_scaled_tier,
    count(*) as cnt,
    min(least_pressure_scaled_rounded_pressure) as min_least_pressure_scaled_rounded_pressure
from cleaned
group by case when pressure_scaled >= 66 then 'LARGE' else 'SMALL' end
having min_least_pressure_scaled_rounded_pressure >= 0;
