WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(voltage, 0.0) AS voltage_filled,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage <= 238.3
      AND quality = 'OK'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage_filled,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        QUARTER(read_at) AS read_at_quarter
    FROM filtered_readings
),
deduped AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage_filled,
        read_at_hour,
        read_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at_quarter, id) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh
    FROM deduped s
)
SELECT id, metric, value
FROM (
    SELECT id, avg_reading_kwh, min_reading_kwh
    FROM profiled
) src
UNPIVOT (
    value FOR metric IN (avg_reading_kwh, min_reading_kwh)
) unp
