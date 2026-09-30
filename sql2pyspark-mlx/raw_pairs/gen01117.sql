WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY status_bits) AS rn
    FROM device_readings
    QUALIFY rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        rn,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY rn, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS humidity_avg_5,
        SUM(temp_c) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c,
        RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST) AS temp_c_rank,
        COUNT(humidity) OVER w_ordered AS device_code_count_humidity
    FROM latest
    WINDOW w_ordered AS (PARTITION BY device_code)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        firmware,
        rn,
        humidity_avg_5,
        cumulative_temp_c,
        CASE WHEN firmware IN ('1.1.0', '1.0.3') THEN 'Group A' ELSE 'Other' END AS firmware_group
    FROM scored
    WHERE temp_c_rank = 6
),
latest_device_readings AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        pressure,
        firmware,
        rn,
        cumulative_temp_c
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS row_num
        FROM base
    ) t
    WHERE row_num = 1
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        pressure,
        cumulative_temp_c,
        COUNT(cumulative_temp_c) OVER (PARTITION BY device_code ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cumulative_temp_c,
        SUM(cumulative_temp_c) OVER (PARTITION BY device_code ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cumulative_temp_c,
        MIN(cumulative_temp_c) OVER (PARTITION BY device_code ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cumulative_temp_c
    FROM latest_device_readings
    WHERE firmware IN ('1.1.0', '2.0.1')
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored_device_readings
    EXCEPT DISTINCT
    SELECT id
    FROM device_readings
    WHERE device_code IN ('DV-02', 'DV-01', 'DV-03')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_device_readings s
    INNER JOIN matched_keys k ON s.id = k.id
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_count_cumulative_temp_c >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_cumulative_temp_c_tier,
        MAX(running_count_cumulative_temp_c) AS max_running_count_cumulative_temp_c,
        APPROX_PERCENTILE(running_min_cumulative_temp_c, ARRAY(0.25, 0.5, 0.75)) AS running_min_cumulative_temp_c_quartiles
    FROM verified
    GROUP BY CASE WHEN running_count_cumulative_temp_c >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT running_count_cumulative_temp_c_tier, max_running_count_cumulative_temp_c
FROM totals
ORDER BY running_count_cumulative_temp_c_tier NULLS LAST, max_running_count_cumulative_temp_c
