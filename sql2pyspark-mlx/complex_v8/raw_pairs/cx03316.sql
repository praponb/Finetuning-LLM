WITH covered AS (
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
    SELECT site_id, MIN(read_at) AS first_reading_read_at, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM covered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        quality,
        rate,
        energy_cost,
        first_reading_read_at,
        reading_count,
        HOUR(first_reading_read_at) AS first_reading_read_at_hour,
        SUBSTRING(TRIM(zone), 1, 3) AS zone_prefix,
        LENGTH(TRIM(quality)) AS quality_len
    FROM summarized_rows
)
-- Final projection
SELECT
    id,
    read_at,
    rate,
    energy_cost,
    first_reading_read_at,
    first_reading_read_at_hour,
    zone_prefix,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
    DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
FROM tagged
WHERE reading_count < 6
ORDER BY id DESC, energy_cost
LIMIT 10
