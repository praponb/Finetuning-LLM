WITH ok_by_site AS (
    -- OK rows per site_id
    SELECT site_id, COUNT(*) AS ok_count, SUM(kwh) AS ok_total_kwh
    FROM meter_readings
    WHERE quality = 'OK'
    GROUP BY site_id
),
bad_by_site AS (
    -- BAD rows per site_id
    SELECT site_id, COUNT(*) AS bad_count, SUM(kwh) AS bad_total_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
    GROUP BY site_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.site_id, b.site_id) AS site_id,
    COALESCE(a.ok_count, 0) AS ok_count,
    a.ok_total_kwh,
    COALESCE(b.bad_count, 0) AS bad_count,
    COALESCE(b.bad_total_kwh, 0) AS bad_total_kwh,
    COALESCE(a.ok_count, 0) - COALESCE(b.bad_count, 0) AS ok_count_minus_bad_count
FROM ok_by_site a
FULL OUTER JOIN bad_by_site b
  ON a.site_id = b.site_id
ORDER BY site_id
