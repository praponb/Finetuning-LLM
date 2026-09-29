with valid as (
    -- Keep recent, valid rows
    select
        id,
        temp_c,
        pressure,
        status_bits,
        firmware,
        humidity * temp_c as humidity_times_temp_c,
        upper(firmware) as firmware_upper
    from device_readings
    where device_code = 'DV-01'
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        temp_c,
        pressure,
        status_bits,
        firmware,
        humidity_times_temp_c,
        firmware_upper,
        ntile(4) over (order by humidity_times_temp_c desc, id) as humidity_times_temp_c_bucket,
        lead(pressure, 2) over (order by temp_c, id) as next_pressure,
        cume_dist() over w_hist as status_bits_cume_dist
    from valid
    window w_hist as (order by status_bits desc)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        first_value(status_bits_cume_dist) ignore nulls over (order by humidity_times_temp_c, id rows between unbounded preceding and current row) as first_known_status_bits_cume_dist,
        row_number() over (order by status_bits desc, id) as event_seq,
        row_number() over (order by status_bits_cume_dist, id) as row_seq
    from scored
)
-- Multi-level aggregation
select
    case when row_seq >= 8 then 'LARGE' else 'SMALL' end as row_seq_tier,
    case when next_pressure >= 1033 then 'LARGE' else 'SMALL' end as next_pressure_tier,
    count(*) as event_count,
    max(first_known_status_bits_cume_dist) as max_first_known_status_bits_cume_dist,
    round(avg(next_pressure), 2) as avg_next_pressure,
    percentile_approx(humidity_times_temp_c, 0.5) as humidity_times_temp_c_p50,
    percentile_approx(status_bits, array(0.25, 0.5, 0.75)) as status_bits_quartiles,
    dense_rank() over (partition by case when row_seq >= 8 then 'LARGE' else 'SMALL' end order by round(avg(next_pressure), 2) desc) as avg_next_pressure_dense_rank
from metrics
group by case when row_seq >= 8 then 'LARGE' else 'SMALL' end, case when next_pressure >= 1033 then 'LARGE' else 'SMALL' end
order by next_pressure_tier desc;
