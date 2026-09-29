WITH valid AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 9
)
SELECT id, kwh, site_id
FROM valid
WHERE MONTH(read_at) IN (1, 3)
UNION
SELECT site_id AS id, voltage AS kwh, id AS site_id
FROM meter_readings
