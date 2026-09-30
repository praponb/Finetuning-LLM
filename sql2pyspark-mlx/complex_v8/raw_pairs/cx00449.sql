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
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_activity_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE kwh > (SELECT MIN(kwh) FROM with_activity)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, si.installed_kw AS site_installed_kw, si.tariff AS site_tariff
    FROM with_activity_2 s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    zone,
    rate,
    site_installed_kw,
    site_tariff
FROM combined_details
ORDER BY zone NULLS LAST, site_id
