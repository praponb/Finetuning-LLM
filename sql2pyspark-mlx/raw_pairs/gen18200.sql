WITH enriched AS (
    SELECT
        id,
        temp_c,
        humidity,
        AVG(humidity) OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY reading_ts, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS humidity_avg_5,
        SUM(humidity) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        STDDEV_SAMP(temp_c) OVER (PARTITION BY device_code) AS device_code_stddev_samp_temp_c
    FROM device_readings
    WHERE humidity BETWEEN 44 AND 61
),
base AS (
    SELECT
        id,
        humidity,
        humidity_avg_5,
        cumulative_humidity,
        ROUND(temp_c * 1.15, 2) AS temp_c_adjusted,
        cumulative_humidity - humidity_avg_5 AS cumulative_humidity_minus_humidity_avg_5
    FROM enriched
    WHERE humidity > 37
),
valid AS (
    SELECT
        id,
        cumulative_humidity,
        temp_c_adjusted,
        cumulative_humidity_minus_humidity_avg_5,
        cumulative_humidity + humidity_avg_5 AS cumulative_humidity_plus_humidity_avg_5
    FROM base
    WHERE temp_c_adjusted > 47
),
scored AS (
    SELECT
        id,
        cumulative_humidity_plus_humidity_avg_5,
        SUM(cumulative_humidity_minus_humidity_avg_5) OVER (ORDER BY cumulative_humidity_plus_humidity_avg_5, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_cumulative_humidity_minus_humidity_avg_5,
        COUNT(cumulative_humidity_minus_humidity_avg_5) OVER (ORDER BY cumulative_humidity_plus_humidity_avg_5, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_cumulative_humidity_minus_humidity_avg_5,
        MAX(cumulative_humidity_minus_humidity_avg_5) OVER (ORDER BY cumulative_humidity_plus_humidity_avg_5, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_cumulative_humidity_minus_humidity_avg_5,
        AVG(cumulative_humidity_minus_humidity_avg_5) OVER (ORDER BY cumulative_humidity_plus_humidity_avg_5, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_cumulative_humidity_minus_humidity_avg_5,
        LAG(cumulative_humidity, 2) OVER (ORDER BY cumulative_humidity_minus_humidity_avg_5, id) AS prev_cumulative_humidity
    FROM valid
),
valid_device_readings AS (
    SELECT *, cumulative_humidity_plus_humidity_avg_5 / NULLIF(last4_max_cumulative_humidity_minus_humidity_avg_5, 0) AS cumulative_humidity_plus_humidity_avg_5_per_last4_max_cumulative_humidity_minus_humidity_avg_5
    FROM scored
    WHERE last4_avg_cumulative_humidity_minus_humidity_avg_5 IS NOT NULL
)
SELECT
    id,
    last4_sum_cumulative_humidity_minus_humidity_avg_5,
    last4_count_cumulative_humidity_minus_humidity_avg_5,
    last4_max_cumulative_humidity_minus_humidity_avg_5,
    last4_avg_cumulative_humidity_minus_humidity_avg_5,
    prev_cumulative_humidity
FROM valid_device_readings
ORDER BY id DESC NULLS LAST
