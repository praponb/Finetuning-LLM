WITH bad_monthly AS (
    -- BAD rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS bad_count, SUM(kwh) AS bad_total_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
    GROUP BY DATE_TRUNC('MONTH', read_at)
),
est_monthly AS (
    -- EST rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS est_count, SUM(kwh) AS est_total_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY DATE_TRUNC('MONTH', read_at)
)
SELECT
    COALESCE(a.read_at_month, b.read_at_month) AS read_at_month,
    COALESCE(a.bad_count, 0) AS bad_count,
    COALESCE(a.bad_total_kwh, 0) AS bad_total_kwh,
    COALESCE(b.est_count, 0) AS est_count,
    b.est_total_kwh,
    COALESCE(a.bad_count, 0) - COALESCE(b.est_count, 0) AS net_count
FROM bad_monthly a
FULL OUTER JOIN est_monthly b
  ON a.read_at_month = b.read_at_month
ORDER BY read_at_month
