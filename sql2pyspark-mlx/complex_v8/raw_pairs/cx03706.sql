WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR read_at < TIMESTAMP '2025-03-02 09:00:00')
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        greatest_kwh_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS row_num
        FROM prepared_readings
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND LENGTH(TRIM(si.site_name)) >= 5
        )
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    greatest_kwh_voltage,
    ABS(kwh - 52.4) AS kwh_distance,
    ROUND(voltage * 0.25, 2) AS voltage_adj
FROM qualified
ORDER BY kwh_distance
