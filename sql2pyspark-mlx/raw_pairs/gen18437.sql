WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUBSTRING(device_code, 1, 3) AS device_code_prefix
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
base_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        status_bits,
        device_code_prefix,
        latest_rank,
        INITCAP(TRIM(device_code)) AS device_code_title
    FROM ranked
    WHERE latest_rank = 1
),
base_device_readings_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(latest_rank, status_bits) AS least_latest_rank_status_bits
    FROM base_device_readings
    WHERE device_code = 'DV-03'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN least_latest_rank_status_bits >= 61 THEN 'High' WHEN least_latest_rank_status_bits >= 38 THEN 'Medium' ELSE 'Low' END AS least_latest_rank_status_bits_level,
        CEIL(status_bits) AS status_bits_whole
    FROM base_device_readings_2
    WHERE status_bits > 5
)
SELECT
    id,
    device_code,
    status_bits,
    device_code_prefix,
    device_code_title,
    least_latest_rank_status_bits_level,
    CASE WHEN device_code RLIKE '^(DV-01|DV-03)$' THEN 'OTHER_ZONE' WHEN device_code RLIKE '^(DV-02|DV-04)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS device_code_zone
FROM filtered
ORDER BY device_code NULLS FIRST
