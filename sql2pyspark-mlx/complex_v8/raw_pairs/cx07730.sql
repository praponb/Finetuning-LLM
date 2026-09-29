WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.zone,
        s.rate,
        s.energy_cost,
        si.commissioned_on AS site_commissioned_on
    FROM matched s
    JOIN sites si ON si.id = s.site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT COUNT(CASE WHEN m.quality IN ('EST', 'OK') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM with_parent_site s
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw BETWEEN 96.9 AND 373.3
              AND si.tariff IN ('TOU', 'EV')
        )
)
SELECT
    id,
    site_id,
    energy_cost,
    site_commissioned_on,
    max_reading_voltage,
    reading_quality_count,
    max_reading_voltage + reading_quality_count AS max_reading_voltage_plus_reading_quality_count
FROM with_activity
ORDER BY energy_cost, max_reading_voltage_plus_reading_quality_count NULLS LAST, id
LIMIT 10
