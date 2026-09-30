WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        firmware,
        pressure / NULLIF(status_bits, 0) AS pressure_per_status_bits
    FROM device_readings
    WHERE reading_ts >= '2025-09-19'
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        status_bits + temp_c AS status_bits_plus_temp_c,
        ROUND(temp_c * 1.15, 2) AS temp_c_adjusted
    FROM filtered
    WHERE device_code NOT IN ('DV-02', 'DV-04', 'DV-03')
),
filtered_device_readings_2 AS (
    -- Keep recent, valid rows
    SELECT *, CAST(pressure AS INT) AS pressure_int
    FROM filtered_device_readings
    WHERE status_bits_plus_temp_c BETWEEN 40 AND 54
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS rn
        FROM filtered_device_readings_2
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(temp_c) OVER (ORDER BY pressure_int, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS temp_c_avg_5_prior,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pressure_avg_5_prior,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY pressure_per_status_bits, id) AS prev_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_plus_temp_c, id) AS row_seq,
        DENSE_RANK() OVER (ORDER BY pressure DESC NULLS FIRST) AS pressure_dense_rank
    FROM deduped
),
kept_keys AS (
    -- Set intersection on keys
    SELECT device_code, firmware
    FROM scored
    INTERSECT
    SELECT device_code, firmware
    FROM device_readings
    WHERE device_code = 'DV-03'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        firmware,
        pressure_per_status_bits,
        status_bits_plus_temp_c,
        temp_c_adjusted,
        pressure_int,
        temp_c_avg_5_prior,
        prev_pressure,
        row_seq,
        pressure_dense_rank,
        LENGTH(device_code) AS device_code_length
    FROM reconciled
    WHERE pressure_avg_5_prior BETWEEN 1002 AND 1012
),
stats AS (
    -- Multi-level aggregation
    SELECT
        device_code,
        BIT_OR(status_bits) AS status_bits_bit_or,
        COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
        BIT_OR(status_bits) AS status_bits_bit_or_2
    FROM cleaned
    GROUP BY device_code
)
SELECT
    device_code,
    status_bits_bit_or,
    status_bits_bit_or_2,
    CASE WHEN device_code RLIKE '^(DV-02|DV-03)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS device_code_zone
FROM stats
ORDER BY status_bits_bit_or_2 NULLS LAST
