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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        zone,
        MAX(read_at) AS last_read_at,
        MAX(voltage) AS max_voltage,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        COUNT(*) AS n_rows
    FROM joined_ranges
    GROUP BY 1, 2
    HAVING AVG(energy_cost) > 90.0
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.site_id,
        s.zone,
        s.last_read_at,
        s.max_voltage,
        s.median_kwh,
        s.n_rows,
        si.installed_kw AS site_installed_kw
    FROM per_group s
    JOIN sites si ON si.id = s.site_id AND si.tariff = 'EV'
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.zone,
        s.last_read_at,
        s.median_kwh,
        s.n_rows,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM with_parent s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    site_id,
    zone,
    last_read_at,
    median_kwh,
    n_rows,
    reading_id,
    reading_voltage,
    reading_quality,
    reading_kwh
FROM exploded_detail_readings
WHERE n_rows <= 9
ORDER BY reading_id, reading_quality
