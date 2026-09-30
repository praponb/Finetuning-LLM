SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        pressure
    FROM device_readings
) src
PIVOT (
    AVG(pressure)
    FOR device_code IN ('DV-01' AS dv_01, 'DV-04' AS dv_04, 'DV-02' AS dv_02)
);
