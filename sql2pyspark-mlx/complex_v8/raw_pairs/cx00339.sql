WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE voltage >= 219.6
      AND read_at < TIMESTAMP '2025-02-24 11:00:00'
),
quality_or_unknown_stats AS (
    -- Avg read_at_month per quality_or_unknown
    SELECT quality_or_unknown, AVG(read_at_month) AS avg_read_at_month
    FROM valid
    GROUP BY quality_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_read_at_month,
        ROUND(s.read_at_month - g.avg_read_at_month, 2) AS read_at_month_minus_avg
    FROM valid s
    JOIN quality_or_unknown_stats g ON g.quality_or_unknown = s.quality_or_unknown
    WHERE s.read_at_month > g.avg_read_at_month
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown_2,
        FLOOR(avg_read_at_month / 2) * 2 AS avg_read_at_month_bucket
    FROM outliers
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_or_unknown,
        s.read_at_month,
        s.avg_read_at_month,
        s.read_at_month_minus_avg,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM scored s
    JOIN meter_readings m ON m.site_id = s.site_id AND TO_DATE(m.read_at) < DATE '2025-03-08'
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.quality_or_unknown,
        s.read_at_month_minus_avg,
        s.reading_id,
        s.reading_read_at,
        m.id AS reading_id_2,
        m.kwh AS reading_kwh_2,
        m.read_at AS reading_read_at_2
    FROM line_level_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND MONTH(m.read_at) IN (1, 3)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        quality_or_unknown,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(100 * COUNT(CASE WHEN quality = 'BAD' THEN 1 END) / COUNT(*), 1) AS pct_quality_bad
    FROM exploded_detail_readings
    GROUP BY 1, 2
),
annotated AS (
    -- Derive helper columns
    SELECT
        site_id,
        quality_or_unknown,
        CASE WHEN quality_or_unknown IN ('UNKNOWN', 'EST') THEN 'group_a' WHEN quality_or_unknown = 'OK' THEN 'group_b' ELSE 'other' END AS quality_or_unknown_group,
        COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
    FROM grouped
)
-- Subtotals with GROUPING SETS
SELECT
    quality_or_unknown,
    site_id,
    GROUPING(quality_or_unknown) AS is_total_quality_or_unknown,
    COUNT(*) AS n_rows
FROM annotated
GROUP BY GROUPING SETS ((quality_or_unknown, site_id), (quality_or_unknown))
