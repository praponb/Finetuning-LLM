WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(reading_ts, 'yyyy-MM') AS reading_ts_label,
        CASE WHEN temp_c >= 37.8 THEN 'High' WHEN temp_c >= 22.4 THEN 'Medium' ELSE 'Low' END AS temp_c_tier
    FROM device_readings
    WHERE TO_DATE(reading_ts) >= '2024-12-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        firmware,
        reading_ts_label,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_avg_2h,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
        CUME_DIST() OVER (ORDER BY temp_c ASC NULLS LAST) AS temp_c_cume_dist
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        firmware,
        reading_ts_label,
        temp_c_cume_dist,
        AVG(pressure) OVER w_part AS pressure_avg_2,
        SUM(temp_c) OVER (ORDER BY temp_c_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c,
        NTILE(4) OVER w_hist AS running_status_bits_bucket,
        MIN(status_bits) OVER (PARTITION BY device_code) AS device_code_min_status_bits
    FROM with_windows
    WINDOW w_part AS (ORDER BY pressure_avg_2h ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY device_code ORDER BY running_status_bits DESC, id ASC)
),
cleaned_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        firmware,
        reading_ts_label,
        pressure_avg_2,
        cumulative_temp_c,
        running_status_bits_bucket,
        device_code_min_status_bits,
        CAST(temp_c_cume_dist AS INT) AS temp_c_cume_dist_int
    FROM enriched
    WHERE temp_c_cume_dist IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY pressure_avg_2 DESC) AS pressure_avg_2_percent_rank,
        SUM(cumulative_temp_c) OVER (ORDER BY temp_c_cume_dist_int ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_temp_c,
        RANK() OVER (ORDER BY device_code_min_status_bits ASC NULLS LAST) AS device_code_min_status_bits_rank,
        SUM(temp_c_cume_dist_int) OVER (ORDER BY temp_c_cume_dist_int ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c_cume_dist_int,
        AVG(pressure_avg_2) OVER (ORDER BY cumulative_temp_c ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pressure_avg_2_avg_5_prior
    FROM cleaned_device_readings
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, firmware
    FROM metrics
    EXCEPT ALL
    SELECT id, firmware
    FROM device_readings
    WHERE pressure BETWEEN 1012 AND 1020
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN remaining_keys k ON s.id = k.id AND s.firmware = k.firmware
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_status_bits_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS running_status_bits_bucket_tier,
        APPROX_PERCENTILE(cumulative_temp_c, ARRAY(0.25, 0.5, 0.75)) AS cumulative_temp_c_quartiles,
        PERCENTILE_APPROX(pressure_avg_2_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS pressure_avg_2_percent_rank_quartiles,
        ROUND(AVG(device_code_min_status_bits_rank), 2) AS avg_device_code_min_status_bits_rank,
        COUNT(DISTINCT firmware) AS distinct_firmwares,
        COUNT(DISTINCT firmware) AS distinct_firmwares_2,
        ROUND(AVG(pressure_avg_2_avg_5_prior), 2) AS avg_pressure_avg_2_avg_5_prior
    FROM reconciled
    GROUP BY CASE WHEN running_status_bits_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    running_status_bits_bucket_tier,
    cumulative_temp_c_quartiles,
    pressure_avg_2_percent_rank_quartiles,
    distinct_firmwares_2
FROM grouped;
