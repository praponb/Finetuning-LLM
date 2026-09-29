WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_bits_count_2h,
        CORR(status_bits, temp_c) OVER (PARTITION BY device_code) AS corr_status_bits_temp_c,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity
    FROM device_readings
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        SUM(corr_status_bits_temp_c) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_status_bits_temp_c,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY corr_status_bits_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        ROUND(running_humidity / SUM(running_humidity) OVER (PARTITION BY device_code), 4) AS running_humidity_share
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_count_2h,
        corr_status_bits_temp_c,
        running_humidity,
        last_known_humidity,
        running_corr_status_bits_temp_c,
        first_known_pressure,
        running_humidity_share,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_humidity ASC, id ASC) AS seq_no,
        ROUND(status_bits_count_2h / SUM(status_bits_count_2h) OVER (PARTITION BY device_code), 4) AS status_bits_count_2h_share,
        ROW_NUMBER() OVER (ORDER BY running_humidity_share ASC, id ASC) AS row_seq,
        LAST_VALUE(humidity) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity_2,
        FIRST_VALUE(corr_status_bits_temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY running_corr_status_bits_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_corr_status_bits_temp_c
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    SUM(CASE WHEN device_code = 'DV-02' THEN pressure ELSE 0 END) AS dv_02_pressure,
    SUM(CASE WHEN device_code = 'DV-02' THEN humidity ELSE 0 END) AS dv_02_humidity,
    APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    COUNT(*) AS event_count_2
FROM scored
GROUP BY 1, 2
ORDER BY activity_day DESC;
