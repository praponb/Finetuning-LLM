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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
joined_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.zone AS site_zone
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    energy_cost,
    site_zone,
    DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
FROM joined_site
WHERE rate >= 2.2
ORDER BY quality DESC, read_at DESC NULLS FIRST
