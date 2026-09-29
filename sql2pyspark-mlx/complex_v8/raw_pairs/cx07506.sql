WITH est_monthly AS (
    -- EST rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS est_count, SUM(kwh) AS est_total_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY DATE_TRUNC('MONTH', read_at)
),
bad_monthly AS (
    -- BAD rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS bad_count, ROUND(AVG(kwh), 2) AS bad_avg_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
    GROUP BY DATE_TRUNC('MONTH', read_at)
)
SELECT
    COALESCE(a.read_at_month, b.read_at_month) AS read_at_month,
    COALESCE(a.est_count, 0) AS est_count,
    COALESCE(a.est_total_kwh, 0) AS est_total_kwh,
    COALESCE(b.bad_count, 0) AS bad_count,
    COALESCE(b.bad_avg_kwh, 0) AS bad_avg_kwh,
    COALESCE(a.est_count, 0) - COALESCE(b.bad_count, 0) AS net_count
FROM est_monthly a
FULL OUTER JOIN bad_monthly b
  ON a.read_at_month = b.read_at_month
ORDER BY read_at_month
