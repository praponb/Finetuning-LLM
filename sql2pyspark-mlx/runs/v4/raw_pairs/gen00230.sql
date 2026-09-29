WITH base AS (
    SELECT *, CASE WHEN firmware IN ('2.0.1', '1.1.0') THEN 'Tier 1' ELSE 'Other' END AS firmware_group
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 11:00:00'
)
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        humidity
    FROM base
) src
PIVOT (
    SUM(humidity) AS amount
    FOR device_code IN ('DV-01' AS dv_01, 'DV-02' AS dv_02)
);
