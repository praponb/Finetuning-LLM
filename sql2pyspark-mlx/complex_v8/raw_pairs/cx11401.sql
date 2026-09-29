WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE quality <> 'BAD'
      AND kwh BETWEEN 28.4 AND 89.8
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM prepared_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw >= 141.6
              AND si.tariff NOT IN ('FLAT', 'EV')
        )
),
site_id_stats AS (
    -- Avg kwh_bucket per site_id
    SELECT site_id, AVG(kwh_bucket) AS avg_kwh_bucket
    FROM qualified
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_kwh_bucket,
        ROUND(s.kwh_bucket - g.avg_kwh_bucket, 2) AS kwh_bucket_minus_avg
    FROM qualified s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh_bucket > g.avg_kwh_bucket
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.kwh_bucket,
        s.avg_kwh_bucket,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM above_average s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        reading_id,
        reading_quality,
        reading_kwh,
        ROUND(CUME_DIST() OVER w_all_by_reading_kwh_ties, 3) AS reading_kwh_cume_dist,
        COUNT(*) OVER (PARTITION BY reading_quality) AS rows_in_reading_quality,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY reading_quality), 2) AS voltage_vs_reading_quality_avg,
        DENSE_RANK() OVER (ORDER BY reading_kwh, id, reading_id) AS reading_kwh_dense_rank,
        ROUND(avg_kwh_bucket / NULLIF(SUM(avg_kwh_bucket) OVER (PARTITION BY reading_quality), 0), 4) AS avg_kwh_bucket_share_of_reading_quality
    FROM line_level
    WINDOW w_all_by_reading_kwh_ties AS (ORDER BY reading_kwh)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(reading_quality), ' ', '_')) AS reading_quality_slug,
        LOWER(REGEXP_REPLACE(TRIM(reading_quality), ' ', '_')) AS reading_quality_slug_2
    FROM with_windows
    WHERE reading_kwh > 14.4
)
-- Final projection
SELECT
    id,
    reading_quality,
    reading_kwh,
    reading_kwh_cume_dist,
    rows_in_reading_quality,
    voltage_vs_reading_quality_avg,
    reading_kwh_dense_rank,
    avg_kwh_bucket_share_of_reading_quality,
    reading_quality_slug,
    reading_quality_slug_2,
    LOWER(REGEXP_REPLACE(TRIM(reading_quality_slug), ' ', '_')) AS reading_quality_slug_slug,
    SUBSTRING(TRIM(reading_quality_slug), 1, 4) AS reading_quality_slug_prefix
FROM derived
WHERE rows_in_reading_quality BETWEEN 2 AND 19
ORDER BY voltage_vs_reading_quality_avg, reading_quality_slug_2 DESC
