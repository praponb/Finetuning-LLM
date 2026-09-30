WITH covered AS (
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
annotated AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(read_at) AS read_at_year,
        ROUND(voltage / NULLIF(energy_cost, 0), 3) AS voltage_per_energy_cost
    FROM covered
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw > 96.9
              AND UPPER(TRIM(si.site_name)) LIKE 'D%'
        )
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        rate,
        energy_cost,
        read_at_year,
        voltage_per_energy_cost,
        MIN(read_at_year) OVER (PARTITION BY read_at_year, site_id) AS read_at_year_site_id_min_read_at_year,
        SIZE(COLLECT_SET(zone) OVER w_read_at_year_site_id) AS distinct_zone_in_read_at_year_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, zone ORDER BY energy_cost, id), 3) AS energy_cost_cume_dist
    FROM filtered
    WINDOW w_read_at_year_site_id AS (PARTITION BY read_at_year, site_id)
)
SELECT
    id,
    rate,
    energy_cost,
    read_at_year,
    read_at_year_site_id_min_read_at_year,
    distinct_zone_in_read_at_year_site_id,
    energy_cost_cume_dist,
    DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
FROM ranked
ORDER BY read_at_year DESC
