WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE kwh > 45.0
      AND quality IN ('BAD', 'EST')
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at_2
    FROM scoped s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        voltage,
        quality,
        quality_len,
        kwh_bucket,
        LAG(kwh) OVER (ORDER BY quality_len, id) AS prev_kwh,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality
    FROM profiled
    WINDOW w_quality AS (PARTITION BY quality)
),
quality_stats AS (
    -- Avg kwh_bucket per quality
    SELECT quality, AVG(kwh_bucket) AS avg_kwh_bucket
    FROM with_windows
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_kwh_bucket,
        ROUND(s.kwh_bucket - g.avg_kwh_bucket, 2) AS kwh_bucket_minus_avg
    FROM with_windows s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh_bucket > g.avg_kwh_bucket
)
SELECT id, measure, amount
FROM (
    SELECT id, voltage, kwh_bucket_minus_avg
    FROM above_average
) src
UNPIVOT (
    amount FOR measure IN (voltage, kwh_bucket_minus_avg)
) unp
ORDER BY id, measure
