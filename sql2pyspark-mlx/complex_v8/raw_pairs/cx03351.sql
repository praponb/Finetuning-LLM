WITH scoped AS (
    -- Filter tariff_windows
    SELECT *
    FROM tariff_windows
    WHERE HOUR(valid_from) BETWEEN 9 AND 11
      AND tariff = 'TOU'
)
SELECT id AS key_id, tariff, rate AS value
FROM scoped
UNION
SELECT id AS key_id, quality AS tariff, voltage AS value
FROM meter_readings
WHERE kwh >= 37.0
  AND voltage <= 236.7
ORDER BY key_id DESC, value
