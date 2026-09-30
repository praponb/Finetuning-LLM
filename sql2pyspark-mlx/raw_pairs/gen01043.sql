WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 08:00:00'
),
base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), reading_ts) / 365 AS years_since_reading_ts,
        YEAR(reading_ts) AS reading_ts_year
    FROM filtered
    WHERE device_code IN ('DV-03', 'DV-04')
),
deduped AS (
    SELECT
        id,
        device_code,
        humidity,
        firmware,
        reading_ts_date,
        years_since_reading_ts,
        reading_ts_year
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) = 1
),
deduped_device_readings AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts_date DESC, id) AS row_num
        FROM deduped
    ) x
    WHERE row_num = 1
)
SELECT
    CASE WHEN humidity >= 82 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    ROUND(SUM(reading_ts_year) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS reading_ts_year_per_dv_01,
    MIN(reading_ts_year) AS min_reading_ts_year,
    APPROX_PERCENTILE(reading_ts_year, ARRAY(0.25, 0.5, 0.75)) AS reading_ts_year_quartiles,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM deduped_device_readings
GROUP BY CASE WHEN humidity >= 82 THEN 'LARGE' ELSE 'SMALL' END
