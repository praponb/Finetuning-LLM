SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS rn
    FROM device_readings
) t
WHERE rn = 1
