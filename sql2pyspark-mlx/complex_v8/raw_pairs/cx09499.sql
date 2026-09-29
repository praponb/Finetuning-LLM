SELECT id AS key_id
FROM tariff_windows
WHERE rate BETWEEN 4.2 AND 7.6
  AND tariff IN ('EV', 'FLAT')
EXCEPT
SELECT site_id
FROM meter_readings
ORDER BY key_id DESC
