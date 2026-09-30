WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        humidity,
        status_bits,
        firmware,
        LOWER(firmware) AS firmware_lower,
        INITCAP(TRIM(device_code)) AS device_code_title
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND pressure <= 1000
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(firmware) AS firmware_length
    FROM cleaned
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        status_bits,
        device_code_title,
        MAX(humidity) OVER w_running AS last7_max_humidity,
        AVG(humidity) OVER w_running AS last7_avg_humidity,
        SUM(humidity) OVER w_running AS last7_sum_humidity,
        MIN(humidity) OVER w_running AS last7_min_humidity
    FROM valid
    WHERE firmware = '1.0.3'
    WINDOW w_running AS (ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(last7_max_humidity, 2) AS last7_max_humidity_rounded
    FROM enriched
    WHERE last7_max_humidity >= 59
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last7_min_humidity >= 66 THEN 'HIGH' ELSE 'LOW' END AS last7_min_humidity_tier,
        CASE WHEN last7_max_humidity >= 82 THEN 'LARGE' ELSE 'SMALL' END AS last7_max_humidity_tier,
        PERCENTILE_APPROX(last7_min_humidity, ARRAY(0.25, 0.5, 0.75)) AS last7_min_humidity_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS event_count,
        SUM(status_bits) AS total_status_bits,
        BIT_AND(status_bits) AS status_bits_bit_and
    FROM base
    GROUP BY CUBE (CASE WHEN last7_min_humidity >= 66 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last7_max_humidity >= 82 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    last7_min_humidity_tier,
    last7_max_humidity_tier,
    last7_min_humidity_quartiles,
    event_count,
    total_status_bits,
    status_bits_bit_and
FROM daily
ORDER BY total_status_bits DESC;
