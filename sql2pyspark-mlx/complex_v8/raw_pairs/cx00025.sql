WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        COUNT(*) AS reading_count,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM joined_ranges s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        reading_count_2,
        TO_DATE(last_reading_read_at) AS last_reading_read_at_date,
        CONCAT_WS(': ', zone, quality) AS zone_quality_label
    FROM profiled
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    rate,
    energy_cost,
    reading_count_2,
    last_reading_read_at_date,
    zone_quality_label
FROM derived
