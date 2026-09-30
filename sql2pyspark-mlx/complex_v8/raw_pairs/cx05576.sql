WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(voltage - 221.8) AS voltage_distance,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 09:00:00'
),
labeled AS (
    -- Derive helper columns
    SELECT *, LEAST(kwh, voltage_distance) AS least_kwh_voltage_distance
    FROM recent_readings
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff NOT IN ('EV', 'TOU')
              AND si.installed_kw BETWEEN 93.3 AND 373.3
        )
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone <> 'Z2'
        )
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.voltage_distance,
        s.read_at_hour,
        s.least_kwh_voltage_distance,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM filtered s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh BETWEEN 37.1 AND 87.1
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.read_at_hour,
        s.reading_id,
        s.reading_kwh,
        s.reading_voltage,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh_2
    FROM exploded_detail_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh BETWEEN 35.0 AND 118.3
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, metric, metric_value
FROM (
    SELECT id, reading_id, reading_voltage, reading_kwh, kwh, reading_kwh_2
    FROM exploded_detail
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (reading_voltage AS m_reading_voltage, reading_kwh AS m_reading_kwh, kwh AS m_kwh, reading_kwh_2 AS m_reading_kwh_2)
) unp
