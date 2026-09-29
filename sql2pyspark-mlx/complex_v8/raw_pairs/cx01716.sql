SELECT zone AS category, id, 'site' AS source
FROM sites
UNION
SELECT quality, site_id, 'reading' AS source
FROM meter_readings
