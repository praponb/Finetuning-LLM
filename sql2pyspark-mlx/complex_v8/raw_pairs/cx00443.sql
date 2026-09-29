SELECT quality AS label, site_id AS qty
FROM meter_readings
UNION ALL
SELECT site_name, id
FROM sites
UNION ALL
SELECT tariff, id
FROM sites
ORDER BY qty, label
