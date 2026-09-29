WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure
    FROM device_readings
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY reading_ts DESC) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        CORR(temp_c, pressure) OVER w_ordered AS corr_temp_c_pressure,
        AVG(pressure) OVER w_ordered AS device_code_avg_pressure,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure
    FROM deduped
    WHERE temp_c >= 23
    WINDOW w_ordered AS (PARTITION BY device_code)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_temp_c,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_temp_c,
        MAX(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_temp_c
    FROM enriched
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT id, pressure
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY first_known_pressure NULLS LAST, id) AS dedup_rank
        FROM with_windows
    ) ranked
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN pressure >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_tier,
    MAX(pressure) AS max_pressure,
    APPROX_PERCENTILE(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles,
    PERCENTILE_APPROX(pressure, 0.9) AS pressure_p90,
    STDDEV_SAMP(pressure) AS pressure_stddev,
    PERCENTILE_APPROX(pressure, 0.95) AS pressure_p95,
    ROUND(AVG(pressure), 2) AS avg_pressure
FROM latest
GROUP BY CASE WHEN pressure >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY pressure_p95 DESC NULLS LAST;
