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
site_id_stats AS (
    -- Max prior_voltage_7d per site_id
    SELECT site_id, MAX(prior_voltage_7d) AS max_prior_voltage_7d
    FROM matched
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_voltage_7d,
        ROUND(s.prior_voltage_7d - g.max_prior_voltage_7d, 2) AS prior_voltage_7d_minus_max
    FROM matched s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_voltage_7d = g.max_prior_voltage_7d
)
SELECT
    id,
    site_id,
    read_at,
    prior_rows_7d,
    prior_voltage_7d,
    max_prior_voltage_7d,
    prior_voltage_7d_minus_max,
    ROUND(prior_voltage_7d / NULLIF(max_prior_voltage_7d, 0), 3) AS prior_voltage_7d_per_max_prior_voltage_7d
FROM vs_group
WHERE TO_DATE(read_at) < DATE '2025-02-18'
