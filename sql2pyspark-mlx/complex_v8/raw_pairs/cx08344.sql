WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.voltage) AS prior_voltage_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM matched s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
      AND s.prior_voltage_3d >= 122.5
)
-- Subtotals with GROUPING SETS
SELECT
    prior_rows_3d,
    site_id,
    GROUPING_ID() AS gid,
    PERCENTILE_APPROX(prior_rows_3d, 0.5) AS median_prior_rows_3d,
    MAX(read_at) AS last_read_at,
    ROUND(SUM(prior_rows_3d) / NULLIF(SUM(prior_voltage_3d), 0), 3) AS prior_rows_3d_to_prior_voltage_3d_ratio
FROM screened
GROUP BY GROUPING SETS ((prior_rows_3d, site_id), (prior_rows_3d), (site_id))
