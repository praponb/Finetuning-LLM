WITH joined_ranges AS (
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
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh,
        (SELECT COUNT(*) FILTER (WHERE m.quality NOT IN ('BAD', 'OK')) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FILTER (WHERE m.quality <> 'OK') FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count_2
    FROM joined_ranges s
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality IS NOT NULL THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count_3,
        (SELECT COUNT(*) FILTER (WHERE m.quality <> 'EST') FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count_4
    FROM profiled s
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    rate,
    min_reading_kwh,
    reading_quality_count,
    reading_quality_count_3,
    reading_quality_count_4,
    DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
    ABS(kwh - 57.3) AS kwh_distance
FROM with_stats
ORDER BY site_id DESC NULLS FIRST
