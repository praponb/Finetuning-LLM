WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage + kwh AS voltage_plus_kwh,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE quality <> 'BAD'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality_prefix,
        MAX(kwh) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS site_id_read_at_to_date_max_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at DESC, id), 3) AS read_at_cume_dist,
        LAST_VALUE(voltage_plus_kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_plus_kwh,
        ROUND(CUME_DIST() OVER w_site_id_by_voltage_plus_kwh, 3) AS voltage_plus_kwh_cume_dist,
        LAG(voltage_plus_kwh) OVER (ORDER BY read_at DESC, id) AS prev_voltage_plus_kwh
    FROM cleaned_readings
    WINDOW w_site_id_by_voltage_plus_kwh AS (PARTITION BY site_id ORDER BY voltage_plus_kwh, id)
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality_prefix,
        s.site_id_read_at_to_date_max_kwh,
        s.read_at_cume_dist,
        s.last_known_voltage_plus_kwh,
        s.voltage_plus_kwh_cume_dist,
        s.prev_voltage_plus_kwh,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM windowed s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        voltage,
        quality_prefix,
        read_at_cume_dist,
        last_known_voltage_plus_kwh,
        prev_voltage_plus_kwh,
        reading_id,
        reading_read_at,
        reading_kwh,
        reading_voltage,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY reading_quality, site_id ORDER BY read_at_cume_dist DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        SUM(voltage) OVER (PARTITION BY reading_quality) AS reading_quality_sum_voltage,
        ROW_NUMBER() OVER (PARTITION BY reading_quality, site_id ORDER BY read_at_cume_dist DESC, id, reading_id) AS seq_no
    FROM with_children
)
SELECT
    reading_id,
    seq_no,
    GROUPING(reading_id) AS is_total_reading_id,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(reading_kwh) / NULLIF(SUM(voltage), 0), 3) AS reading_kwh_to_voltage_ratio
FROM sequenced
GROUP BY ROLLUP(reading_id, seq_no)
ORDER BY reading_id NULLS LAST, seq_no NULLS LAST
