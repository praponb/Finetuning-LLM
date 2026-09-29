WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h
    FROM meter_readings
    WHERE (quality IS NOT NULL OR voltage <= 231.6)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE kwh > (SELECT AVG(kwh) FROM prepared)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_id ORDER BY kwh ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        ROUND(kwh_minus_voltage - AVG(kwh_minus_voltage) OVER (PARTITION BY quality), 2) AS kwh_minus_voltage_vs_quality_avg,
        ROUND(AVG(kwh_minus_voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_minus_voltage_avg_7,
        SUM(kwh) OVER (PARTITION BY site_id, quality ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior,
        LAST_VALUE(kwh_minus_voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_minus_voltage
    FROM filtered
),
latest AS (
    -- Keep the earliest row per quality, site_id
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh_minus_voltage,
        read_at_plus_2h,
        kwh_avg_2,
        kwh_minus_voltage_vs_quality_avg,
        kwh_minus_voltage_avg_7,
        kwh_sum_2_prior,
        last_known_kwh_minus_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY kwh_sum_2_prior, id) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh_minus_voltage,
        read_at_plus_2h,
        kwh_avg_2,
        kwh_minus_voltage_vs_quality_avg,
        kwh_minus_voltage_avg_7,
        kwh_sum_2_prior,
        last_known_kwh_minus_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh_sum_2_prior, id) AS row_num
        FROM latest
    ) ranked
    WHERE row_num = 1
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.read_at_plus_2h,
        s.kwh_avg_2,
        s.kwh_minus_voltage_vs_quality_avg,
        s.kwh_sum_2_prior,
        s.last_known_kwh_minus_voltage,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM unique_rows s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
combined_details_reading AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.kwh_avg_2,
        s.kwh_minus_voltage_vs_quality_avg,
        s.kwh_sum_2_prior,
        s.last_known_kwh_minus_voltage,
        s.reading_id,
        s.reading_read_at,
        s.reading_voltage,
        m.site_id AS reading_site_id,
        m.read_at AS reading_read_at_2,
        m.quality AS reading_quality_2
    FROM detail_rows s
    LEFT JOIN meter_readings m ON m.id = s.reading_id AND m.quality NOT IN ('BAD', 'EST')
)
SELECT
    id,
    kwh,
    voltage,
    kwh_avg_2,
    kwh_minus_voltage_vs_quality_avg,
    kwh_sum_2_prior,
    last_known_kwh_minus_voltage,
    reading_read_at,
    reading_voltage,
    reading_site_id,
    reading_read_at_2,
    reading_quality_2,
    reading_read_at_2 + INTERVAL 7 DAYS AS reading_read_at_2_plus_7d
FROM combined_details_reading
ORDER BY last_known_kwh_minus_voltage NULLS LAST, reading_read_at
