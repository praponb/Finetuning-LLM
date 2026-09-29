WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 10:00:00'
      AND quality = 'BAD'
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM base s
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        reading_count,
        site_id,
        ROUND(AVG(reading_count), 2) AS avg_reading_count,
        SUM(kwh) AS total_kwh,
        SUM(reading_count) AS total_reading_count,
        COUNT(*) FILTER (WHERE quality <> 'EST') AS n_quality_est,
        ROUND(100 * COUNT(CASE WHEN quality NOT IN ('EST', 'OK') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_est_ok,
        MAX(read_at) AS last_read_at
    FROM with_counts
    WHERE voltage <= 240.1
    GROUP BY reading_count, site_id
    HAVING SUM(voltage) > 218.2
),
annotated AS (
    -- Add calculated columns
    SELECT
        reading_count,
        site_id,
        avg_reading_count,
        total_kwh,
        n_quality_est,
        pct_quality_not_in_est_ok,
        DATE_TRUNC('DAY', last_read_at) AS last_read_at_day,
        CASE WHEN n_quality_est >= 6 THEN 'HIGH' WHEN n_quality_est >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS n_quality_est_tier
    FROM totals
    WHERE site_id IS NOT NULL
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh > 26.9
        )
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        COUNT(*) AS reading_count_2,
        MIN(read_at) AS first_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_count_2, 0) AS reading_count_2,
        m.first_reading_read_at_2
    FROM screened s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.reading_count,
        s.site_id,
        s.avg_reading_count,
        s.total_kwh,
        s.n_quality_est,
        s.pct_quality_not_in_est_ok,
        s.n_quality_est_tier,
        s.first_reading_read_at,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM profiled s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    reading_count,
    site_id,
    avg_reading_count,
    total_kwh,
    n_quality_est,
    pct_quality_not_in_est_ok,
    n_quality_est_tier,
    first_reading_read_at,
    reading_id,
    reading_kwh,
    reading_quality,
    reading_read_at,
    LOWER(REGEXP_REPLACE(TRIM(n_quality_est_tier), ' ', '_')) AS n_quality_est_tier_slug,
    ROUND(avg_reading_count * 0.25, 2) AS avg_reading_count_adj
FROM line_level_readings
WHERE pct_quality_not_in_est_ok <= 60.0
ORDER BY n_quality_est DESC NULLS FIRST, reading_read_at DESC
