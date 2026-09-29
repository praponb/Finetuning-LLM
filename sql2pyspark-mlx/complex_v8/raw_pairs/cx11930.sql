WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE kwh >= 51.0
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM cleaned_readings s
),
enriched AS (
    -- Attach site attributes
    SELECT s.*, si.site_name
    FROM profiled s
    JOIN sites si ON si.id = s.site_id
),
enriched_site AS (
    -- Attach site attributes
    SELECT s.*, si.commissioned_on AS site_commissioned_on
    FROM enriched s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    reading_count_2,
    reading_count,
    site_id,
    ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio
FROM enriched_site
GROUP BY ROLLUP(reading_count_2, reading_count, site_id)
ORDER BY reading_count_2 NULLS LAST, reading_count NULLS LAST, site_id NULLS LAST
