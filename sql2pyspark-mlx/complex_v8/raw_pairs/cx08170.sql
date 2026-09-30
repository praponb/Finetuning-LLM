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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh,
        MAX(kwh) AS max_reading_kwh,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh,
        m.max_reading_kwh,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM matched s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.zone,
        s.avg_reading_kwh,
        s.max_reading_kwh,
        s.reading_count,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM summarized_rows s
    JOIN sites si ON si.id = s.site_id
),
deduped AS (
    -- Keep the latest row per zone, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        zone,
        avg_reading_kwh,
        max_reading_kwh,
        reading_count,
        site_zone,
        site_tariff
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone, site_id ORDER BY read_at DESC, id DESC) AS rn
        FROM with_details_site
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    avg_reading_kwh,
    max_reading_kwh,
    reading_count,
    site_zone,
    site_tariff,
    CASE WHEN zone IN ('Z4', 'Z2') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group
FROM deduped
ORDER BY site_zone
