WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-02'
      AND quality IN ('OK', 'BAD')
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM scoped s
    JOIN meter_readings m ON m.site_id = s.site_id
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.reading_id,
        s.reading_kwh,
        s.reading_voltage,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM with_children s
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        reading_id,
        reading_kwh,
        reading_voltage,
        site_name,
        site_zone,
        ROUND(reading_voltage / NULLIF(voltage, 0), 3) AS reading_voltage_per_voltage
    FROM combined_details
    WHERE site_id IS NOT NULL
),
enriched_reading AS (
    -- Join meter_readings
    SELECT s.*, m.site_id AS reading_site_id, m.kwh AS reading_kwh_2
    FROM enriched s
    JOIN meter_readings m ON m.id = s.reading_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM enriched_reading s
    LEFT JOIN reading_stats m ON m.site_id = s.reading_site_id
)
-- Final projection
SELECT
    id,
    read_at,
    reading_id,
    reading_kwh,
    site_name,
    reading_voltage_per_voltage,
    reading_site_id,
    reading_kwh_2,
    reading_count
FROM summarized_rows
ORDER BY reading_id NULLS LAST, site_name
