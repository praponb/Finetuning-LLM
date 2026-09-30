with valid as (
    -- Keep recent, valid rows
    select
        id,
        temp_c,
        status_bits,
        firmware,
        coalesce(pressure, 0) pressure_filled
    from device_readings
    where temp_c >= 33
),
valid_device_readings as (
    -- Filter the raw rows and derive helper columns
    select *, round(status_bits * 0.9, 2) status_bits_adjusted
    from valid
    where pressure_filled between 68 and 69
),
matched_keys as (
    -- Set difference on keys
    select firmware, id
    from valid_device_readings
    except all
    select firmware, id
    from device_readings
    where status_bits = 8
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid_device_readings s
    inner join matched_keys k on s.firmware = k.firmware and s.id = k.id
),
valid_device_readings_2 as (
    -- Keep recent, valid rows
    select
        temp_c,
        status_bits,
        firmware,
        pressure_filled,
        status_bits_adjusted,
        trim(firmware) clean_firmware
    from verified
    where status_bits between 11 and 12
),
metrics as (
    -- Ranking and gap features
    select *, dense_rank() over (order by status_bits_adjusted nulls last) status_bits_adjusted_dense_rank
    from valid_device_readings_2
)
-- Multi-level aggregation
select
    case when status_bits_adjusted >= 75 then 'HIGH' else 'LOW' end as status_bits_adjusted_tier,
    case when pressure_filled >= 70 then 'LARGE' else 'SMALL' end as pressure_filled_tier,
    case when status_bits_adjusted_dense_rank >= 9 then 'HIGH' else 'LOW' end as status_bits_adjusted_dense_rank_tier,
    bit_and(status_bits) as status_bits_bit_and,
    round(avg(status_bits_adjusted_dense_rank), 2) as avg_status_bits_adjusted_dense_rank,
    sum(status_bits) as total_status_bits,
    bit_and(status_bits) as status_bits_bit_and_2,
    percentile_approx(status_bits_adjusted, array(0.25, 0.5, 0.75)) as status_bits_adjusted_quartiles,
    avg(pressure_filled) as avg_pressure_filled
from metrics
group by 1, 2, 3
order by status_bits_adjusted_dense_rank_tier desc
