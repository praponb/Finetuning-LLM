WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(humidity, 0) AS humidity_rounded
    FROM device_readings
    WHERE humidity < 53
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c NULLS LAST, id) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity_rounded,
        SUM(status_bits) OVER w_ordered AS last3_sum_status_bits,
        MAX(status_bits) OVER w_ordered AS last3_max_status_bits,
        COUNT(status_bits) OVER w_ordered AS last3_count_status_bits
    FROM latest
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY latest_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, last3_count_status_bits * pressure AS last3_count_status_bits_times_pressure
    FROM metrics
    WHERE last3_max_status_bits < 8.5
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last3_count_status_bits_times_pressure, 1) OVER (ORDER BY humidity, id) AS prev_last3_count_status_bits_times_pressure,
        LAST_VALUE(pressure) IGNORE NULLS OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure,
        LAG(last3_count_status_bits_times_pressure, 2) OVER (ORDER BY pressure, id) AS prev_last3_count_status_bits_times_pressure_2,
        SUM(temp_c) OVER (ORDER BY last3_max_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c,
        SUM(temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c_2
    FROM cleaned
    WHERE humidity_rounded IS NULL
),
scored_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(prev_last3_count_status_bits_times_pressure_2) OVER w_hist AS last4_count_prev_last3_count_status_bits_times_pressure_2,
        SUM(prev_last3_count_status_bits_times_pressure_2) OVER w_hist AS last4_sum_prev_last3_count_status_bits_times_pressure_2,
        MIN(prev_last3_count_status_bits_times_pressure_2) OVER w_hist AS last4_min_prev_last3_count_status_bits_times_pressure_2,
        MAX(prev_last3_count_status_bits_times_pressure_2) OVER w_hist AS last4_max_prev_last3_count_status_bits_times_pressure_2,
        FIRST_VALUE(last3_count_status_bits, TRUE) OVER (ORDER BY last3_sum_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_count_status_bits
    FROM scored
    WINDOW w_hist AS (ORDER BY last3_count_status_bits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN temp_c >= 35.1 THEN 'LARGE' ELSE 'SMALL' END AS temp_c_tier,
    CASE WHEN pressure >= 1019 THEN 'LARGE' ELSE 'SMALL' END AS pressure_tier,
    PERCENTILE_APPROX(status_bits, ARRAY(0.25, 0.5, 0.75)) AS status_bits_quartiles,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    ROUND(AVG(last4_max_prev_last3_count_status_bits_times_pressure_2), 2) AS avg_last4_max_prev_last3_count_status_bits_times_pressure_2,
    APPROX_PERCENTILE(last4_sum_prev_last3_count_status_bits_times_pressure_2, 0.95) AS last4_sum_prev_last3_count_status_bits_times_pressure_2_p95,
    APPROX_PERCENTILE(last_known_pressure, ARRAY(0.25, 0.5, 0.75)) AS last_known_pressure_quartiles
FROM scored_device_readings
GROUP BY CASE WHEN temp_c >= 35.1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN pressure >= 1019 THEN 'LARGE' ELSE 'SMALL' END
