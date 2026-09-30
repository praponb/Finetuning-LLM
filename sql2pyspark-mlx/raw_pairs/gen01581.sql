with with_windows as (
    select
        *,
        max(humidity) over w_hist as device_code_max_humidity,
        avg(status_bits) over w_hist_2 as status_bits_avg_5_prior
    from device_readings
    where firmware in ('1.0.3', '1.1.0', '2.0.1')
    window w_hist as (partition by device_code), w_hist_2 as (partition by device_code order by pressure, id rows between 5 preceding and 1 preceding)
),
cleaned as (
    select
        *,
        length(firmware) as firmware_length,
        least(device_code_max_humidity, status_bits_avg_5_prior) as least_device_code_max_humidity_status_bits_avg_5_prior
    from with_windows
    where device_code = 'DV-04'
),
enriched as (
    select
        *,
        lag(least_device_code_max_humidity_status_bits_avg_5_prior) over (partition by device_code order by device_code_max_humidity, id) as prev_least_device_code_max_humidity_status_bits_avg_5_prior,
        lead(least_device_code_max_humidity_status_bits_avg_5_prior) over (partition by device_code order by device_code_max_humidity, id) as next_least_device_code_max_humidity_status_bits_avg_5_prior,
        lead(pressure) over (partition by device_code order by device_code_max_humidity, id) as next_pressure,
        lag(pressure) over (partition by device_code order by device_code_max_humidity, id) as prev_pressure,
        row_number() over (partition by device_code order by device_code_max_humidity, id) as row_seq,
        round(pressure / sum(pressure) over (partition by device_code), 4) as pressure_share
    from cleaned
),
base as (
    select
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        device_code_max_humidity,
        status_bits_avg_5_prior,
        firmware_length,
        least_device_code_max_humidity_status_bits_avg_5_prior,
        next_least_device_code_max_humidity_status_bits_avg_5_prior,
        next_pressure,
        prev_pressure,
        row_seq,
        pressure_share,
        humidity + next_pressure as humidity_plus_next_pressure
    from enriched
    where device_code_max_humidity = 46
),
metrics as (
    select
        *,
        round(device_code_max_humidity / sum(device_code_max_humidity) over (partition by device_code), 4) as device_code_max_humidity_share,
        corr(temp_c, status_bits) over (partition by device_code) as corr_temp_c_status_bits,
        lag(prev_pressure, 1) over (order by pressure_share, id) as prev_prev_pressure,
        ntile(10) over (partition by device_code order by humidity_plus_next_pressure desc, id) as humidity_plus_next_pressure_bucket
    from base
),
scored as (
    select
        *,
        count(prev_prev_pressure) over w_grp as last7_count_prev_prev_pressure,
        max(prev_prev_pressure) over w_grp as last7_max_prev_prev_pressure,
        min(prev_prev_pressure) over w_grp as last7_min_prev_prev_pressure,
        stddev_samp(next_least_device_code_max_humidity_status_bits_avg_5_prior) over (partition by device_code) as device_code_stddev_samp_next_least_device_code_max_humidity_status_bits_avg_5_prior
    from metrics
    window w_grp as (partition by device_code order by pressure_share, id rows between 6 preceding and current row)
),
daily as (
    select
        device_code,
        percentile_approx(next_pressure, 0.9) as next_pressure_p90,
        count(*) filter (where device_code = 'DV-01') as dv_01_rows,
        count(case when device_code = 'DV-04' then 1 end) as dv_04_count,
        round(sum(humidity) / nullif(count(case when device_code = 'DV-01' then 1 end), 0), 2) as humidity_per_dv_01,
        approx_percentile(humidity_plus_next_pressure_bucket, array(0.25, 0.5, 0.75)) as humidity_plus_next_pressure_bucket_quartiles,
        count(*) as num_rows
    from scored
    group by device_code
)
select
    device_code,
    next_pressure_p90,
    humidity_per_dv_01,
    num_rows,
    case when device_code rlike '^(DV-03|DV-04)$' then 'OTHER_ZONE' when device_code rlike '^(DV-01|DV-02)$' then 'EXTENDED' else 'GROUP_B' end as device_code_zone
from daily;
