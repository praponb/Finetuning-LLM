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
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.energy_cost,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM in_window s
    JOIN sites si ON si.id = s.site_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        quality,
        energy_cost,
        site_installed_kw,
        FLOOR(energy_cost / 250) * 250 AS energy_cost_bucket,
        ADD_MONTHS(site_commissioned_on, 6) AS site_commissioned_on_plus_6m
    FROM joined
    WHERE energy_cost <= 630.0
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        quality,
        energy_cost,
        site_installed_kw,
        energy_cost_bucket,
        site_commissioned_on_plus_6m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, site_installed_kw, energy_cost
    FROM latest_per_key
) src
UNPIVOT (
    reading FOR measure IN (site_installed_kw AS m_site_installed_kw, energy_cost AS energy_cost_v)
) unp
