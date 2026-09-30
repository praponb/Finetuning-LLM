SELECT id AS ref_id
FROM meter_readings
WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
EXCEPT
SELECT id AS ref_id
FROM sites
WHERE commissioned_on < DATE '2025-05-10'
  AND site_name IS NOT NULL
ORDER BY ref_id DESC
