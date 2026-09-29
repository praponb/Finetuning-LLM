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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM joined_ranges s
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        zone,
        rate,
        energy_cost,
        reading_count,
        reading_count_2,
        LENGTH(TRIM(zone)) AS zone_len,
        ROUND(kwh * 0.9, 2) AS kwh_adj,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM profiled
    WHERE HOUR(read_at) BETWEEN 6 AND 8
)
SELECT
    id,
    site_id,
    zone,
    rate,
    energy_cost,
    reading_count,
    reading_count_2,
    zone_len,
    kwh_adj,
    zone_slug
FROM derived
ORDER BY kwh_adj DESC, reading_count NULLS LAST
