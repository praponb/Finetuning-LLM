WITH covered AS (
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
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DAYOFWEEK(read_at) AS read_at_dayofweek,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM covered
)
-- Final projection
SELECT
    id,
    site_id,
    prior_voltage_12h,
    read_at_dayofweek,
    read_at_label
FROM derived
WHERE read_at_dayofweek BETWEEN 2 AND 5
ORDER BY site_id DESC NULLS FIRST, id
LIMIT 10
