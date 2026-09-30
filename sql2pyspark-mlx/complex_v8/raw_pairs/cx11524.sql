WITH in_window AS (
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
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.zone,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM in_window s
    JOIN meter_readings m ON m.site_id = s.site_id
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM exploded_detail
    WHERE kwh > (SELECT MIN(kwh) FROM exploded_detail)
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.zone,
        s.reading_id,
        s.reading_read_at,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    reading_id,
    reading_read_at,
    site_installed_kw,
    site_name
FROM with_parent_site
