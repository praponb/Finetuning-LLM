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
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined_ranges
    WHERE energy_cost > (SELECT AVG(energy_cost) FROM joined_ranges)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM with_activity s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    energy_cost,
    reading_count,
    ROUND(reading_count / NULLIF(kwh, 0), 3) AS reading_count_per_kwh
FROM with_stats
WHERE zone IS NOT NULL
ORDER BY read_at DESC NULLS FIRST, site_id DESC
