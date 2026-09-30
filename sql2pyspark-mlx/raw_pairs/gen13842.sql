WITH enriched AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        ROW_NUMBER() OVER w_recent AS seq_no,
        LAG(temp_c, 2, 0) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_temp_c,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY TO_DATE(reading_ts), device_code), 4) AS pressure_share,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pressure_avg_2_prior
    FROM device_readings
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY temp_c DESC, id)
),
valid AS (
    SELECT
        id,
        temp_c,
        pressure,
        status_bits,
        firmware,
        seq_no,
        prev_temp_c,
        pressure_share,
        pressure_avg_2_prior,
        ROUND(pressure_share * 1.1, 2) AS adjusted_pressure_share,
        status_bits / NULLIF(pressure_share, 0) AS status_bits_per_pressure_share
    FROM enriched
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 07:00:00')
),
valid_device_readings AS (
    SELECT
        id,
        temp_c,
        pressure,
        firmware,
        seq_no,
        prev_temp_c,
        pressure_share,
        pressure_avg_2_prior,
        adjusted_pressure_share,
        status_bits_per_pressure_share,
        ROUND(temp_c * 1.07, 2) AS temp_c_adjusted
    FROM valid
    WHERE prev_temp_c >= 34
),
filtered AS (
    SELECT
        id,
        pressure,
        seq_no,
        pressure_avg_2_prior,
        adjusted_pressure_share,
        temp_c_adjusted,
        CASE WHEN adjusted_pressure_share >= 87 THEN 'High' WHEN adjusted_pressure_share >= 27 THEN 'Medium' ELSE 'Low' END AS adjusted_pressure_share_level
    FROM valid_device_readings
    WHERE pressure < 1022
),
valid_device_readings_2 AS (
    SELECT
        id,
        pressure,
        seq_no,
        pressure_avg_2_prior,
        adjusted_pressure_share,
        temp_c_adjusted,
        adjusted_pressure_share_level,
        adjusted_pressure_share + pressure_avg_2_prior AS adjusted_pressure_share_plus_pressure_avg_2_prior
    FROM filtered
    WHERE seq_no BETWEEN 5 AND 7
)
SELECT
    CASE WHEN adjusted_pressure_share >= 64 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_pressure_share_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(adjusted_pressure_share_plus_pressure_avg_2_prior), 2) AS avg_adjusted_pressure_share_plus_pressure_avg_2_prior
FROM valid_device_readings_2
GROUP BY CASE WHEN adjusted_pressure_share >= 64 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_adjusted_pressure_share_plus_pressure_avg_2_prior DESC NULLS FIRST;
