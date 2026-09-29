WITH bad_by_site AS (
    -- BAD rows per site_id
    SELECT site_id, COUNT(*) AS bad_count, SUM(kwh) AS bad_total_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
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
    COALESCE(a.bad_count, 0) AS bad_count,
    COALESCE(a.bad_total_kwh, 0) AS bad_total_kwh,
    COALESCE(b.ok_count, 0) AS ok_count,
    b.ok_total_kwh,
    COALESCE(a.bad_count, 0) - COALESCE(b.ok_count, 0) AS bad_count_minus_ok_count
FROM bad_by_site a
FULL OUTER JOIN ok_by_site b
  ON a.site_id = b.site_id
ORDER BY site_id
