WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        YEAR(read_at) AS read_at_year,
        QUARTER(read_at) AS read_at_quarter
    FROM meter_readings
    WHERE site_id <= 3
      AND quality IN ('OK', 'EST')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage, id) AS rn,
        ROUND(read_at_quarter / NULLIF(SUM(read_at_quarter) OVER (PARTITION BY read_at_year), 0), 4) AS read_at_quarter_share_of_read_at_year,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY read_at_year)) AS distinct_site_id_in_read_at_year,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage) AS voltage_rank,
        FIRST_VALUE(read_at_quarter) OVER (PARTITION BY quality, site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at_quarter
    FROM prepared_readings
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage
    FROM sequenced s
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    quality_slug,
    read_at_year,
    read_at_quarter,
    rn,
    distinct_site_id_in_read_at_year,
    last_reading_read_at,
    max_reading_voltage,
    LEAST(first_read_at_quarter, kwh) AS least_first_read_at_quarter_kwh,
    LEAST(read_at_year, first_read_at_quarter) AS least_read_at_year_first_read_at_quarter
FROM profiled
ORDER BY least_read_at_year_first_read_at_quarter DESC
