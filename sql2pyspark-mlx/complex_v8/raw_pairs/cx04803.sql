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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(energy_cost) OVER (PARTITION BY site_id) AS site_id_sum_energy_cost,
        MAX(rate) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rate_max_7
    FROM joined_ranges
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.rate,
        s.energy_cost,
        s.site_id_sum_energy_cost,
        s.rate_max_7,
        si.installed_kw AS site_installed_kw
    FROM with_trends s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff NOT IN ('FLAT', 'TOU')
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.rate,
        s.site_id_sum_energy_cost,
        s.rate_max_7,
        si.site_name,
        si.installed_kw AS site_installed_kw_2,
        si.tariff AS site_tariff
    FROM with_parent s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    site_name,
    site_installed_kw_2
FROM with_details_site
ORDER BY site_installed_kw_2, site_name DESC NULLS FIRST
