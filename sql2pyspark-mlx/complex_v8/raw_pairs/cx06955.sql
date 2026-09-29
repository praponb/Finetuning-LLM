WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.voltage) AS prior_voltage_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw < 373.3
              AND UPPER(TRIM(si.site_name)) LIKE 'H%'
        )
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND m.voltage <= 240.1
        )
)
-- Final projection
SELECT
    id,
    read_at,
    prior_rows_7d,
    prior_voltage_7d,
    FLOOR(prior_voltage_7d / 250) * 250 AS prior_voltage_7d_bucket,
    GREATEST(prior_rows_7d, prior_voltage_7d) AS greatest_prior_rows_7d_prior_voltage_7d
FROM without_matches
ORDER BY read_at DESC NULLS FIRST
