with filtered as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits + temp_c status_bits_plus_temp_c,
        least(temp_c, pressure) least_temp_c_pressure
    from device_readings
    where humidity >= 46
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by pressure desc nulls last, id) as latest_rank
    from filtered
    qualify latest_rank = 1
),
remaining_keys as (
    -- Set difference on keys
    select device_code
    from latest
    except
    select device_code
    from device_readings
    where device_code in ('DV-04', 'DV-02', 'DV-03')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join remaining_keys k on s.device_code = k.device_code
),
enriched as (
    -- Ranking and gap features
    select *, cume_dist() over (partition by device_code order by least_temp_c_pressure desc nulls last) least_temp_c_pressure_cume_dist
    from verified
)
select
    device_code,
    pressure,
    status_bits,
    least_temp_c_pressure,
    latest_rank
from enriched;
