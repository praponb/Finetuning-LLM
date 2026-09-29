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
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff IN ('FLAT', 'EV')
              AND si.installed_kw BETWEEN 96.9 AND 386.6
        )
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.energy_cost,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM filtered s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh BETWEEN 14.4 AND 110.5
)
SELECT
    id,
    read_at,
    voltage,
    energy_cost,
    reading_id,
    reading_kwh,
    reading_read_at,
    CASE WHEN voltage >= 238.3 THEN 'A' WHEN voltage >= 220.7 THEN 'B' ELSE 'C' END AS voltage_tier
FROM detail_rows
ORDER BY id, reading_read_at DESC
