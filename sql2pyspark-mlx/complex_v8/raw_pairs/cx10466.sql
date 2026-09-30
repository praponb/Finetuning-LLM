WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage > 224.8
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        ROUND(SUM(voltage) / NULLIF(SUM(voltage_bucket), 0), 3) AS voltage_to_voltage_bucket_ratio,
        SUM(voltage) AS total_voltage
    FROM prepared_readings
    WHERE MONTH(read_at) IN (1, 3)
    GROUP BY 1
),
with_details_site AS (
    -- Attach site attributes
    SELECT s.site_id, s.voltage_to_voltage_bucket_ratio, si.site_name
    FROM stats s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
)
-- Final projection
SELECT site_id, voltage_to_voltage_bucket_ratio, site_name
FROM with_details_site
WHERE LOWER(site_name) LIKE '%no%'
ORDER BY site_id DESC NULLS FIRST, site_name DESC NULLS FIRST
