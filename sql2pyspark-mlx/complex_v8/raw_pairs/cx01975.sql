SELECT id, site_name AS label, 'site' AS record_type
FROM sites
UNION ALL
SELECT site_id AS id, quality AS label, 'reading' AS record_type
FROM meter_readings
UNION ALL
SELECT id, tariff AS label, 'site_3' AS record_type
FROM sites
WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-05-10'
  AND zone = 'Z4'
ORDER BY id, label
