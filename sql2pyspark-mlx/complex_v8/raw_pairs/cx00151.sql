WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(voltage * 1.07, 2) AS voltage_adj,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE quality <> 'OK'
),
unique_rows AS (
    -- Keep the latest row per quality_or_unknown, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        voltage_adj,
        quality_or_unknown,
        quality_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality_or_unknown, site_id ORDER BY voltage_adj DESC, id DESC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.voltage_adj,
        s.quality_or_unknown,
        s.quality_prefix,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM unique_rows s
    JOIN meter_readings m ON m.site_id = s.site_id
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage_adj,
        s.quality_or_unknown,
        s.quality_prefix,
        s.reading_id,
        s.reading_quality,
        s.reading_kwh,
        m.id AS reading_id_2,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at_2,
        m.quality AS reading_quality_2
    FROM exploded_detail_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT *
FROM (
    SELECT reading_id_2, reading_read_at_2, reading_quality, voltage_adj
    FROM exploded_detail
) src
PIVOT (
    SUM(voltage_adj)
    FOR reading_quality IN ('BAD', 'OK')
)
ORDER BY reading_id_2, reading_read_at_2
