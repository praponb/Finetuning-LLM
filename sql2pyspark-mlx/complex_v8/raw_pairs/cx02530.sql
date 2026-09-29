WITH in_window AS (
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
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone NOT IN ('Z4', 'Z3')
              AND LENGTH(TRIM(si.site_name)) >= 5
        )
)
SELECT
    site_id,
    prior_rows_7d,
    GROUPING_ID() AS grouping_level,
    MIN(prior_rows_7d) AS min_prior_rows_7d,
    ROUND(SUM(prior_voltage_7d), 2) AS total_prior_voltage_7d,
    ROUND(SUM(prior_voltage_7d) / NULLIF(SUM(prior_rows_7d), 0), 3) AS prior_voltage_7d_to_prior_rows_7d_ratio
FROM screened
GROUP BY ROLLUP(site_id, prior_rows_7d)
ORDER BY site_id NULLS LAST, prior_rows_7d NULLS LAST
