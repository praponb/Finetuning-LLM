WITH in_window AS (
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
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.zone,
        s.rate,
        s.energy_cost,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM in_window s
    LEFT JOIN sites si ON si.id = s.site_id AND si.installed_kw >= 93.3
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(kwh) AS min_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_kwh
    FROM joined_site s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
unique_rows AS (
    -- Keep the latest row per site_tariff, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        zone,
        rate,
        energy_cost,
        site_installed_kw,
        site_tariff,
        min_reading_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_tariff, site_id ORDER BY rate DESC, id DESC) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT site_id, site_tariff, energy_cost
    FROM unique_rows
) src
PIVOT (
    SUM(energy_cost) AS amount, COUNT(energy_cost) AS n
    FOR site_tariff IN ('EV', 'FLAT')
)
