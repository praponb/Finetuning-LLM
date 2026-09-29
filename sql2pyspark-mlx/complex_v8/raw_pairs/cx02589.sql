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
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw BETWEEN 93.3 AND 446.9
              AND LENGTH(TRIM(si.site_name)) >= 7
        )
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.zone,
        s.energy_cost,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM filtered s
    JOIN meter_readings m ON m.site_id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MAX(read_at) AS last_reading_read_at,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at,
        m.first_reading_read_at
    FROM detail_rows_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    zone,
    energy_cost,
    reading_quality,
    reading_kwh,
    last_reading_read_at,
    ROUND((UNIX_TIMESTAMP(first_reading_read_at) - UNIX_TIMESTAMP(read_at)) / 3600, 1) AS hours_read_at_to_first_reading_read_at,
    CASE WHEN energy_cost >= 585.0 THEN 'HIGH' WHEN energy_cost >= 225.0 THEN 'MEDIUM' ELSE 'LOW' END AS energy_cost_tier
FROM summarized_rows
ORDER BY id
