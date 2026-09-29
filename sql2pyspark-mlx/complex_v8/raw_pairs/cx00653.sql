SELECT site_id, kwh AS amount
FROM meter_readings
UNION ALL
SELECT id AS site_id, installed_kw AS amount
FROM sites
WHERE installed_kw >= 141.6
  AND tariff IN ('FLAT', 'TOU')
