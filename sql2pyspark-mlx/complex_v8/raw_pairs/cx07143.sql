WITH joined_ranges AS (
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
derived AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM joined_ranges
    WHERE TO_DATE(read_at) < DATE '2025-02-24'
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 233.1
              AND HOUR(m.read_at) BETWEEN 8 AND 11
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_3d,
    prior_voltage_3d,
    read_at_day
FROM screened
WHERE prior_voltage_3d >= 428.8
