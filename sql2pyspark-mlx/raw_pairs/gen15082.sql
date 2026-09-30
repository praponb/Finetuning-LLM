WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(temp_c, pressure) greatest_temp_c_pressure
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(temp_c) OVER w_grp last4_min_temp_c,
        SUM(temp_c) OVER w_grp last4_sum_temp_c,
        MAX(temp_c) OVER w_grp last4_max_temp_c,
        RANK() OVER (ORDER BY pressure NULLS LAST) pressure_rank
    FROM valid
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        last4_max_temp_c + pressure_rank last4_max_temp_c_plus_pressure_rank,
        CASE WHEN status_bits >= 12 THEN 'High' WHEN status_bits >= 3 THEN 'Medium' ELSE 'Low' END status_bits_level
    FROM with_windows
    WHERE pressure BETWEEN 1016 AND 1019
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, device_code
    FROM filtered
    EXCEPT
    SELECT id, device_code
    FROM device_readings
    WHERE status_bits > 10
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id AND s.device_code = k.device_code
),
filtered_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN pressure >= 1022 THEN 'High' WHEN pressure >= 999 THEN 'Medium' ELSE 'Low' END pressure_band
    FROM verified
    WHERE humidity >= 49
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        pressure,
        last4_min_temp_c,
        pressure_rank,
        last4_max_temp_c_plus_pressure_rank,
        pressure_band,
        COUNT(last4_min_temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) last4_min_temp_c_count_30m,
        RANK() OVER (PARTITION BY device_code ORDER BY greatest_temp_c_pressure DESC NULLS LAST) greatest_temp_c_pressure_rank
    FROM filtered_device_readings
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        pressure,
        last4_min_temp_c,
        pressure_rank,
        last4_min_temp_c_count_30m,
        greatest_temp_c_pressure_rank,
        DENSE_RANK() OVER (ORDER BY pressure DESC NULLS FIRST) pressure_dense_rank
    FROM scored
)
SELECT
    last4_min_temp_c,
    pressure_rank,
    greatest_temp_c_pressure_rank
FROM scored_device_readings;
