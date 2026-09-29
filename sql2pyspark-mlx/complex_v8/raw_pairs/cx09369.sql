SELECT tariff, rate AS amount, id
FROM tariff_windows
WHERE MONTH(valid_to) IN (2, 3)
  AND tariff IN ('EV', 'TOU')
UNION ALL
SELECT quality AS tariff, voltage AS amount, site_id AS id
FROM meter_readings
UNION ALL
SELECT quality AS tariff, voltage AS amount, site_id AS id
FROM meter_readings
WHERE voltage >= 222.3
  AND quality <> 'EST'
