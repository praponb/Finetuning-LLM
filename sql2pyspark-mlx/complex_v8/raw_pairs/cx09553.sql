SELECT id, quality, kwh
FROM meter_readings
UNION ALL
SELECT id, zone, installed_kw
FROM sites
WHERE site_name IS NOT NULL
  AND installed_kw BETWEEN 96.9 AND 446.9
UNION ALL
SELECT id, quality, kwh
FROM meter_readings
WHERE kwh < 101.2
ORDER BY quality, kwh
