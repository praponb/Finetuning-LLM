with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        temp_c,
        humidity,
        firmware,
        concat(firmware, ' ', device_code) as firmware_device_code
    from device_readings
    where reading_ts >= date '2025-09-01'
),
matched_keys as (
    -- Set intersection on keys
    select firmware, device_code
    from filtered
    intersect
    select firmware, device_code
    from device_readings
    where pressure is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.firmware = k.firmware and s.device_code = k.device_code
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        humidity,
        firmware,
        firmware_device_code,
        round(temp_c * 1.07, 2) as adjusted_temp_c,
        round(temp_c * 1.07, 2) as adjusted_temp_c_2
    from kept_rows
    where device_code != 'DV-01'
),
scored as (
    -- Ranking and gap features
    select
        id,
        device_code,
        firmware,
        count(adjusted_temp_c) over (partition by device_code) as device_code_count_adjusted_temp_c,
        avg(adjusted_temp_c) over (partition by device_code) as device_code_avg_adjusted_temp_c,
        percent_rank() over (partition by device_code order by adjusted_temp_c_2 desc) as adjusted_temp_c_2_percent_rank,
        avg(humidity) over (partition by device_code) as device_code_avg_humidity,
        stddev_samp(adjusted_temp_c_2) over (partition by device_code) as device_code_stddev_samp_adjusted_temp_c_2
    from base
    where device_code != 'DV-02'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        device_code,
        firmware,
        adjusted_temp_c_2_percent_rank,
        device_code_stddev_samp_adjusted_temp_c_2,
        max(adjusted_temp_c_2_percent_rank) over (partition by device_code) as device_code_max_adjusted_temp_c_2_percent_rank,
        round(device_code_avg_humidity / sum(device_code_avg_humidity) over (partition by device_code), 4) as device_code_avg_humidity_share,
        max(adjusted_temp_c_2_percent_rank) over (partition by device_code) as device_code_max_adjusted_temp_c_2_percent_rank_2,
        rank() over (partition by device_code order by device_code_stddev_samp_adjusted_temp_c_2 asc nulls last) as device_code_stddev_samp_adjusted_temp_c_2_rank,
        max(device_code_stddev_samp_adjusted_temp_c_2) over (partition by device_code) as device_code_max_device_code_stddev_samp_adjusted_temp_c_2
    from scored
)
select
    id,
    device_code,
    firmware,
    adjusted_temp_c_2_percent_rank,
    device_code_stddev_samp_adjusted_temp_c_2,
    device_code_avg_humidity_share,
    device_code_max_adjusted_temp_c_2_percent_rank_2,
    device_code_stddev_samp_adjusted_temp_c_2_rank
from enriched
order by device_code_stddev_samp_adjusted_temp_c_2 asc;
