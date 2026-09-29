WITH est_by_site AS (
    -- EST rows per site_id
    SELECT site_id, COUNT(*) AS est_count, ROUND(AVG(kwh), 2) AS est_avg_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY site_id
),
ok_by_site AS (
    -- OK rows per site_id
    SELECT site_id, COUNT(*) AS ok_count, SUM(kwh) AS ok_total_kwh
    FROM meter_readings
    WHERE quality = 'OK'
    GROUP BY site_id
)
SELECT
    COALESCE(a.site_id, b.site_id) AS site_id,
    COALESCE(a.est_count, 0) AS est_count,
    a.est_avg_kwh,
    COALESCE(b.ok_count, 0) AS ok_count,
    COALESCE(b.ok_total_kwh, 0) AS ok_total_kwh,
    COALESCE(a.est_count, 0) - COALESCE(b.ok_count, 0) AS est_count_minus_ok_count
FROM est_by_site a
FULL OUTER JOIN ok_by_site b
  ON a.site_id = b.site_id
ORDER BY site_id
