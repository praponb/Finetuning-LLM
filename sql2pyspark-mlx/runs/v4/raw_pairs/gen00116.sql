WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) AS row_seq
    FROM device_readings
)
SELECT
    id,
    device_code,
    temp_c,
    humidity,
    pressure,
    status_bits,
    first_known_temp_c,
    row_seq,
    CASE WHEN device_code RLIKE '^(DV-01|DV-03)$' THEN 'GROUP_B' WHEN device_code RLIKE '^(DV-02|DV-04)$' THEN 'EXTENDED' ELSE 'CORE' END AS device_code_zone
FROM enriched
ORDER BY device_code_zone DESC NULLS FIRST;
