WITH bad_monthly AS (
    -- BAD rows per read_at_month
    SELECT DATE_TRUNC('MONTH', read_at) AS read_at_month, COUNT(*) AS bad_count, ROUND(AVG(kwh), 2) AS bad_avg_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
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
    COALESCE(a.bad_count, 0) AS bad_count,
    COALESCE(a.bad_avg_kwh, 0) AS bad_avg_kwh,
    COALESCE(b.ok_count, 0) AS ok_count,
    COALESCE(b.ok_avg_kwh, 0) AS ok_avg_kwh,
    COALESCE(a.bad_count, 0) - COALESCE(b.ok_count, 0) AS bad_count_minus_ok_count
FROM bad_monthly a
FULL OUTER JOIN ok_monthly b
  ON a.read_at_month = b.read_at_month
ORDER BY read_at_month
