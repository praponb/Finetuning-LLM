WITH ok_monthly AS (
    -- OK rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS ok_count, SUM(kwh) AS ok_total_kwh
    FROM meter_readings
    WHERE quality = 'OK'
    GROUP BY DATE_TRUNC('MONTH', read_at)
),
est_monthly AS (
    -- EST rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS est_count, ROUND(AVG(kwh), 2) AS est_avg_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY DATE_TRUNC('MONTH', read_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.read_at_month, b.read_at_month) AS read_at_month,
    COALESCE(a.ok_count, 0) AS ok_count,
    a.ok_total_kwh,
    COALESCE(b.est_count, 0) AS est_count,
    COALESCE(b.est_avg_kwh, 0) AS est_avg_kwh,
    COALESCE(a.ok_count, 0) - COALESCE(b.est_count, 0) AS net_count
FROM ok_monthly a
FULL OUTER JOIN est_monthly b
  ON a.read_at_month = b.read_at_month
ORDER BY read_at_month
