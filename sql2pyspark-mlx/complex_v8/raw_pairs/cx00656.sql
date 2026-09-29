WITH est_monthly AS (
    -- EST rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS est_count, SUM(kwh) AS est_total_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY DATE_TRUNC('MONTH', read_at)
),
ok_monthly AS (
    -- OK rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS ok_count, ROUND(AVG(kwh), 2) AS ok_avg_kwh
    FROM meter_readings
    WHERE quality = 'OK'
    GROUP BY DATE_TRUNC('MONTH', read_at)
)
SELECT
    COALESCE(a.read_at_month, b.read_at_month) AS read_at_month,
    COALESCE(a.est_count, 0) AS est_count,
    a.est_total_kwh,
    COALESCE(b.ok_count, 0) AS ok_count,
    b.ok_avg_kwh,
    COALESCE(a.est_count, 0) - COALESCE(b.ok_count, 0) AS net_count
FROM est_monthly a
FULL OUTER JOIN ok_monthly b
  ON a.read_at_month = b.read_at_month
ORDER BY read_at_month
