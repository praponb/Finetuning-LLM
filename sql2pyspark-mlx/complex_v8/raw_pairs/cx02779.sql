WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff,
        si.site_name
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
)
SELECT id, measure, metric_value
FROM (
    SELECT id, kwh, voltage
    FROM enriched
) src
UNPIVOT (
    metric_value FOR measure IN (kwh, voltage)
) unp
