WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(firmware)) AS firmware_title
    FROM device_readings
    WHERE temp_c <= 34
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_title,
        FIRST_VALUE(temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_sum_30m
    FROM filtered
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        status_bits,
        firmware,
        firmware_title,
        status_bits_sum_30m,
        DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts,
        MONTH(reading_ts) AS reading_ts_month
    FROM enriched
    WHERE TO_DATE(reading_ts) >= '2025-01-01'
)
-- Grouped metrics
SELECT
    CASE WHEN temp_c >= 34.5 THEN 'LARGE' ELSE 'SMALL' END AS temp_c_tier,
    device_code,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    AVG(temp_c) FILTER (WHERE device_code = 'DV-04') AS dv_04_avg_temp_c
FROM base
GROUP BY ROLLUP (CASE WHEN temp_c >= 34.5 THEN 'LARGE' ELSE 'SMALL' END, device_code)
ORDER BY device_code DESC NULLS FIRST;
