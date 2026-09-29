WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE kwh > 51.5
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(days_since_read_at / NULLIF(kwh, 0), 3) AS days_since_read_at_per_kwh
    FROM cleaned_readings
),
site_id_stats AS (
    -- Avg kwh per site_id
    SELECT site_id, AVG(kwh) AS avg_kwh
    FROM tagged
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM tagged s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh > g.avg_kwh
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.days_since_read_at,
        s.days_since_read_at_per_kwh,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM compared s
    JOIN meter_readings m ON m.site_id = s.site_id
),
latest_per_key AS (
    -- Keep the latest row per site_id, reading_quality
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        days_since_read_at,
        days_since_read_at_per_kwh,
        reading_id,
        reading_read_at,
        reading_voltage,
        reading_quality,
        reading_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, reading_quality ORDER BY days_since_read_at_per_kwh DESC, id DESC, reading_id DESC) AS row_num
        FROM line_level
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        voltage,
        days_since_read_at,
        reading_id,
        reading_read_at,
        reading_voltage,
        reading_quality,
        QUARTER(read_at) AS read_at_quarter,
        CASE WHEN days_since_read_at >= 525 THEN 'A' WHEN days_since_read_at >= 315 THEN 'B' ELSE 'C' END AS days_since_read_at_tier,
        SUBSTRING(TRIM(reading_quality), 1, 2) AS reading_quality_prefix
    FROM latest_per_key
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', reading_read_at) AS reading_read_at_month,
        COUNT(*) AS row_count,
        ROUND(STDDEV(days_since_read_at), 2) AS stddev_days_since_read_at,
        ROUND(100 * COUNT(CASE WHEN reading_quality IN ('EST', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_reading_quality_in_est_bad,
        SUM(read_at_quarter) FILTER (WHERE reading_quality NOT IN ('OK', 'BAD')) AS read_at_quarter_reading_quality_not_in_ok_ba
    FROM annotated
    WHERE read_at_quarter > 2
    GROUP BY 1
)
SELECT
    reading_read_at_month,
    stddev_days_since_read_at,
    pct_reading_quality_in_est_bad
FROM stats
