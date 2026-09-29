WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        ROUND(voltage * 1.07, 2) AS voltage_adj
    FROM meter_readings
    WHERE kwh >= 30.2
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_adj,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM recent s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT *
FROM (
    SELECT reading_id, site_id, quality, kwh
    FROM detail_rows_readings
) src
PIVOT (
    MAX(kwh)
    FOR quality IN ('BAD', 'OK')
)
