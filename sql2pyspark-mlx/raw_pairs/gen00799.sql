SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        humidity
    FROM device_readings
) src
PIVOT (
    AVG(humidity) AS value_sum,
    COUNT(1) AS cnt
    FOR device_code IN ('DV-01' AS dv_01, 'DV-04' AS dv_04)
);
