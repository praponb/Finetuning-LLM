WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 11:00:00'
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.read_at_quarter,
        s.quality_clean,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM recent s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh >= 14.5
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        voltage,
        quality,
        reading_id,
        LOWER(REGEXP_REPLACE(TRIM(quality_clean), ' ', '_')) AS quality_clean_slug,
        ROUND(read_at_quarter / NULLIF(reading_kwh, 0), 3) AS read_at_quarter_per_reading_kwh,
        voltage + read_at_quarter AS voltage_plus_read_at_quarter
    FROM detail_rows
    WHERE reading_id IS NOT NULL
),
stats AS (
    -- Summarise per group
    SELECT
        CASE WHEN voltage >= 231.7 THEN 'large' WHEN voltage >= 219.6 THEN 'medium' ELSE 'small' END AS voltage_tier,
        quality,
        reading_id,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(voltage) / NULLIF(SUM(read_at_quarter_per_reading_kwh), 0), 3) AS voltage_to_read_at_quarter_per_reading_kwh_ratio,
        ROUND(SUM(voltage) / NULLIF(SUM(voltage_plus_read_at_quarter), 0), 3) AS voltage_to_voltage_plus_read_at_quarter_ratio
    FROM scored
    GROUP BY CASE WHEN voltage >= 231.7 THEN 'large' WHEN voltage >= 219.6 THEN 'medium' ELSE 'small' END, quality, reading_id
),
exploded AS (
    -- One row per quality_list_item
    SELECT
        s.voltage_tier,
        s.quality,
        s.reading_id,
        s.voltage_to_read_at_quarter_per_reading_kwh_ratio,
        s.voltage_to_voltage_plus_read_at_quarter_ratio,
        t.quality_list_item_pos,
        t.quality_list_item
    FROM stats s
    LATERAL VIEW POSEXPLODE(s.quality_list) t AS quality_list_item_pos, quality_list_item
)
SELECT
    voltage_tier,
    reading_id,
    voltage_to_read_at_quarter_per_reading_kwh_ratio,
    voltage_to_voltage_plus_read_at_quarter_ratio,
    quality_list_item_pos,
    quality_list_item,
    CONCAT(quality, '#', CAST(reading_id AS STRING)) AS quality_ref
FROM exploded
ORDER BY quality_ref DESC
