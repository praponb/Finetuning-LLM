SELECT
    id,
    site_name AS name,
    installed_kw,
    'site' AS origin
FROM sites
WHERE zone <> 'Z3'
UNION ALL
SELECT
    id,
    quality,
    voltage,
    'reading' AS origin
FROM meter_readings
WHERE voltage <= 229.1
  AND quality = 'OK'
ORDER BY id DESC
