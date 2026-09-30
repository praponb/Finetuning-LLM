WITH cleaned AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        pressure * temp_c AS pressure_times_temp_c,
        CONCAT(firmware, ' ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 03:00:00'
),
deduped AS (
    SELECT
        id,
        device_code,
        firmware,
        firmware_device_code
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS latest_rank
        FROM cleaned
    ) x
    WHERE latest_rank = 1
),
filtered AS (
    SELECT
        *,
        CASE WHEN device_code IN ('DV-03', 'DV-02') THEN 'Priority' ELSE 'Other' END AS device_code_group,
        CONCAT_WS(',', firmware, device_code) AS firmware_device_code_key
    FROM deduped
    WHERE firmware = '1.0.3'
),
cleaned_device_readings AS (
    SELECT *, CASE WHEN device_code IN ('DV-04', 'DV-01') THEN 'Tier 1' ELSE 'Other' END AS device_code_group_2
    FROM filtered
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
),
base AS (
    SELECT
        *,
        CASE WHEN device_code IN ('DV-01', 'DV-02') THEN 'Group A' ELSE 'Other' END AS device_code_group_3,
        INITCAP(TRIM(device_code)) AS device_code_title
    FROM cleaned_device_readings
    WHERE device_code IN ('DV-01', 'DV-02', 'DV-03')
),
filtered_device_readings AS (
    SELECT
        id,
        device_code,
        firmware,
        firmware_device_code,
        device_code_group,
        SUBSTRING(firmware, 1, 3) AS firmware_prefix,
        TRIM(device_code) AS clean_device_code
    FROM base
    WHERE firmware IN ('1.1.0', '1.0.3')
)
SELECT
    device_code,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events,
    COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes_2,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events_2
FROM filtered_device_readings
GROUP BY 1
HAVING dv_02_events < 1000
