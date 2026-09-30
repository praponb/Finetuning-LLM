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
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh BETWEEN 28.4 AND 113.1
              AND m.quality <> 'EST'
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    site_commissioned_on,
    MONTH(site_commissioned_on) AS site_commissioned_on_month,
    DATE_TRUNC('QUARTER', site_commissioned_on) AS site_commissioned_on_quarter
FROM enriched
ORDER BY id DESC, site_commissioned_on_month DESC
