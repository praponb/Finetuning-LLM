WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.voltage) AS prior_voltage_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_12h,
        s.prior_voltage_12h,
        si.site_name
    FROM matched s
    JOIN sites si ON si.id = s.site_id
),
site_id_stats AS (
    -- Avg prior_rows_12h per site_id
    SELECT site_id, AVG(prior_rows_12h) AS avg_prior_rows_12h
    FROM with_parent
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_12h,
        ROUND(s.prior_rows_12h - g.avg_prior_rows_12h, 2) AS prior_rows_12h_minus_avg
    FROM with_parent s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_rows_12h > g.avg_prior_rows_12h
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_12h,
    prior_voltage_12h,
    avg_prior_rows_12h,
    prior_rows_12h_minus_avg,
    UPPER(TRIM(site_name)) AS site_name_clean,
    DATE_TRUNC('MONTH', read_at) AS read_at_month
FROM with_activity
ORDER BY site_id DESC, id
LIMIT 10
