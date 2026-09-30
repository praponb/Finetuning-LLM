WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 09:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_kwh,
        s.quality_slug,
        si.site_name
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_kwh,
        si.zone AS site_zone
    FROM joined s
    JOIN sites si ON si.id = s.site_id
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_kwh,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM combined_details_site s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    reading_id,
    site_id,
    ROUND(STDDEV(voltage_minus_kwh), 2) AS stddev_voltage_minus_kwh
FROM with_children_readings
GROUP BY ROLLUP(reading_id, site_id)
