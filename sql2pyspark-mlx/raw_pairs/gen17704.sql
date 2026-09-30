SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        pressure
    FROM device_readings
) src
PIVOT (
    AVG(pressure) AS total
    FOR device_code IN ('DV-03' AS dv_03, 'DV-04' AS dv_04)
);
