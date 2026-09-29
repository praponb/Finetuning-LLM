WITH est_by_site AS (
    -- EST rows per site_id
    SELECT site_id, COUNT(*) AS est_count, SUM(kwh) AS est_total_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY site_id
),
bad_by_site AS (
    -- BAD rows per site_id
    SELECT site_id, COUNT(*) AS bad_count, ROUND(AVG(kwh), 2) AS bad_avg_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
    GROUP BY site_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.site_id, b.site_id) AS site_id,
    COALESCE(a.est_count, 0) AS est_count,
    COALESCE(a.est_total_kwh, 0) AS est_total_kwh,
    COALESCE(b.bad_count, 0) AS bad_count,
    COALESCE(b.bad_avg_kwh, 0) AS bad_avg_kwh,
    COALESCE(a.est_count, 0) - COALESCE(b.bad_count, 0) AS net_count
FROM est_by_site a
FULL OUTER JOIN bad_by_site b
  ON a.site_id = b.site_id
ORDER BY site_id
