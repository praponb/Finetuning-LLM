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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality <> 'OK'
        )
      AND HOUR(s.read_at) BETWEEN 10 AND 13
),
joined_site AS (
    -- Attach site attributes
    SELECT s.*, si.zone AS site_zone, si.installed_kw AS site_installed_kw
    FROM without_matches s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 93.3
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        quality,
        zone,
        ROUND(SUM(energy_cost), 2) AS total_energy_cost,
        MAX(read_at) AS last_read_at,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM joined_site
    GROUP BY DATE_TRUNC('MONTH', read_at), quality, zone
)
SELECT *
FROM (
    SELECT read_at_month, zone, total_energy_cost
    FROM rollup_base
) src
PIVOT (
    MAX(total_energy_cost)
    FOR zone IN ('Z4', 'Z2', 'Z3')
)
ORDER BY read_at_month
