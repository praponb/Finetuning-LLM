WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CAST(temp_c AS INT) AS temp_c_int
    FROM device_readings
    WHERE temp_c > 21
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        pressure,
        status_bits,
        temp_c_int,
        MAX(humidity) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_humidity,
        SUM(humidity) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_humidity,
        COUNT(humidity) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_humidity,
        FIRST_VALUE(pressure, TRUE) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure
    FROM valid
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(first_known_pressure) OVER w_running AS last3_avg_first_known_pressure,
        COUNT(first_known_pressure) OVER w_running AS last3_count_first_known_pressure,
        MAX(first_known_pressure) OVER w_running AS last3_max_first_known_pressure
    FROM enriched
    WINDOW w_running AS (ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        running_sum_humidity,
        first_known_pressure,
        last3_count_first_known_pressure,
        last3_max_first_known_pressure,
        SUM(pressure) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure,
        AVG(running_max_humidity) OVER w_running AS running_max_humidity_avg_3_prior,
        SUM(running_max_humidity) OVER (ORDER BY temp_c_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_humidity
    FROM scored
    WINDOW w_running AS (ORDER BY first_known_pressure, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
-- Grouped metrics
SELECT
    CASE WHEN running_sum_humidity >= 330 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_humidity_tier,
    CASE WHEN last3_max_first_known_pressure >= 1028 THEN 'HIGH' ELSE 'LOW' END AS last3_max_first_known_pressure_tier,
    ROUND(AVG(first_known_pressure), 2) AS avg_first_known_pressure,
    PERCENTILE_APPROX(running_pressure, ARRAY(0.25, 0.5, 0.75)) AS running_pressure_quartiles,
    PERCENTILE_APPROX(running_running_max_humidity, ARRAY(0.25, 0.5, 0.75)) AS running_running_max_humidity_quartiles,
    GROUPING(CASE WHEN running_sum_humidity >= 330 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN running_sum_humidity >= 330 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last3_max_first_known_pressure >= 1028 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN running_sum_humidity >= 330 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY running_sum_humidity_tier DESC
