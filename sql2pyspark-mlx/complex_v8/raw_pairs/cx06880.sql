WITH matched AS (
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
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone <> 'Z2'
              AND si.commissioned_on >= DATE '2024-09-15'
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, SUM(voltage) AS total_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.total_reading_voltage
    FROM without_matches s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
scored AS (
    -- Derive helper columns
    SELECT *, TO_DATE(read_at) AS read_at_date
    FROM with_counts
    WHERE total_reading_voltage <= 1155.0
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    rate,
    energy_cost,
    reading_count,
    total_reading_voltage,
    read_at + INTERVAL 2 HOURS AS read_at_plus_2h
FROM scored
ORDER BY voltage DESC NULLS FIRST, read_at
