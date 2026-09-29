WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
      AND quality IN ('EST', 'BAD')
)
SELECT id AS qty, read_at
FROM valid_readings
UNION
SELECT id AS qty, valid_to AS read_at
FROM tariff_windows
WHERE tariff = 'TOU'
ORDER BY read_at, qty
