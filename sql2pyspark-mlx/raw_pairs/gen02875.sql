with valid as (
    -- Basic cleansing and defaults
    select *, concat(device_code, ' - ', firmware) device_code_firmware
    from device_readings
    where firmware = '1.0.3'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        round(humidity / sum(humidity) over (partition by device_code), 4) humidity_share,
        round(humidity / sum(humidity) over (partition by device_code), 4) humidity_share_2,
        row_number() over (partition by device_code order by reading_ts desc, id) event_seq
    from valid
),
filtered as (
    -- Keep recent, valid rows
    select *, cast(temp_c as int) temp_c_int
    from with_windows
    where humidity <= 67
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        first_value(event_seq) ignore nulls over (order by temp_c, id rows between unbounded preceding and current row) first_known_event_seq,
        avg(humidity_share_2) over w_running humidity_share_2_avg_5,
        row_number() over (partition by device_code order by humidity_share desc, id) event_seq_2,
        avg(humidity_share_2) over (partition by device_code) device_code_avg_humidity_share_2
    from filtered
    window w_running as (partition by device_code order by humidity, id rows between 5 preceding and current row)
),
totals as (
    -- Grouped metrics
    select
        case when status_bits >= 11 then 'HIGH' else 'LOW' end as status_bits_tier,
        case when first_known_event_seq >= 8 then 'PREMIUM' else 'STANDARD' end as first_known_event_seq_tier,
        bit_or(status_bits) as status_bits_bit_or,
        round(sum(humidity_share) / nullif(count(case when device_code = 'DV-03' then 1 end), 0), 2) as humidity_share_per_dv_03,
        approx_percentile(first_known_event_seq, 0.95) as first_known_event_seq_p95
    from metrics
    group by grouping sets ((case when status_bits >= 11 then 'HIGH' else 'LOW' end, case when first_known_event_seq >= 8 then 'PREMIUM' else 'STANDARD' end), (case when status_bits >= 11 then 'HIGH' else 'LOW' end), ())
    having status_bits_bit_or > 1
)
select
    first_known_event_seq_tier,
    status_bits_bit_or,
    humidity_share_per_dv_03
from totals
order by status_bits_bit_or desc nulls first, humidity_share_per_dv_03;
