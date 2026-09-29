WITH est_monthly AS (
    -- EST rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS est_count, SUM(kwh) AS est_total_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY DATE_TRUNC('MONTH', read_at)
),
ok_monthly AS (
    -- OK rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS ok_count, SUM(kwh) AS ok_total_kwh
    FROM meter_readings
    WHERE quality = 'OK'
    GROUP BY DATE_TRUNC('MONTH', read_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.read_at_month, b.read_at_month) AS read_at_month,
    COALESCE(a.est_count, 0) AS est_count,
    COALESCE(a.est_total_kwh, 0) AS est_total_kwh,
    COALESCE(b.ok_count, 0) AS ok_count,
    COALESCE(b.ok_total_kwh, 0) AS ok_total_kwh,
    COALESCE(a.est_count, 0) - COALESCE(b.ok_count, 0) AS est_count_minus_ok_count
FROM est_monthly a
FULL OUTER JOIN ok_monthly b
  ON a.read_at_month = b.read_at_month
ORDER BY read_at_month
