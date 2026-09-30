WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.voltage) AS prior_voltage_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        prior_rows_1d,
        ROUND(AVG(prior_voltage_1d), 2) AS avg_prior_voltage_1d,
        PERCENTILE_APPROX(prior_voltage_1d, 0.5) AS median_prior_voltage_1d
    FROM joined_ranges
    GROUP BY TO_DATE(read_at), site_id, prior_rows_1d
    HAVING AVG(prior_voltage_1d) > 245.0
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.read_at_day,
        s.site_id,
        s.prior_rows_1d,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM rollup_base s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    read_at_day,
    site_id,
    prior_rows_1d,
    reading_id,
    reading_read_at
FROM detail_rows
ORDER BY site_id, reading_id
