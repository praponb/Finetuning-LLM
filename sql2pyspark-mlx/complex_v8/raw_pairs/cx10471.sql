WITH ok_by_site AS (
    -- OK rows per site_id
    SELECT site_id, COUNT(*) AS ok_count, ROUND(AVG(kwh), 2) AS ok_avg_kwh
    FROM meter_readings
    WHERE quality = 'OK'
    GROUP BY site_id
),
est_by_site AS (
    -- EST rows per site_id
    SELECT site_id, COUNT(*) AS est_count, ROUND(AVG(kwh), 2) AS est_avg_kwh
    FROM meter_readings
    WHERE quality = 'EST'
    GROUP BY site_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.site_id, b.site_id) AS site_id,
    COALESCE(a.ok_count, 0) AS ok_count,
    a.ok_avg_kwh,
    COALESCE(b.est_count, 0) AS est_count,
    b.est_avg_kwh,
    COALESCE(a.ok_count, 0) - COALESCE(b.est_count, 0) AS net_count
FROM ok_by_site a
FULL OUTER JOIN est_by_site b
  ON a.site_id = b.site_id
ORDER BY site_id
