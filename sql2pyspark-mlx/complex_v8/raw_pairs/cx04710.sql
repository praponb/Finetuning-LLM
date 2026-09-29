WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour_2
    FROM meter_readings
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        site_id,
        SUM(CASE WHEN quality NOT IN ('BAD', 'OK') THEN 1 ELSE 0 END) AS n_quality_not_in_bad_ok,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM filtered
    WHERE quality = 'OK'
    GROUP BY DATE_FORMAT(read_at, 'yyyy-MM'), site_id
    HAVING MAX(voltage) > 218.5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        read_at_ym,
        site_id,
        quality_list,
        ROW_NUMBER() OVER (PARTITION BY n_quality_not_in_bad_ok, site_id ORDER BY n_quality_not_in_bad_ok DESC, read_at_ym, site_id) AS rn,
        LAG(n_quality_not_in_bad_ok, 1, 0) OVER (PARTITION BY n_quality_not_in_bad_ok, site_id ORDER BY n_quality_not_in_bad_ok DESC, read_at_ym, site_id) AS prev_n_quality_not_in_bad_ok
    FROM per_group
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on IS NOT NULL
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM with_activity s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN rn >= 8 THEN 'A' WHEN rn >= 3 THEN 'B' ELSE 'C' END AS rn_tier,
        last_reading_read_at + INTERVAL 30 MINUTES AS last_reading_read_at_plus_30m
    FROM profiled
)
SELECT
    read_at_ym,
    site_id,
    quality_list,
    rn,
    prev_n_quality_not_in_bad_ok,
    last_reading_read_at,
    rn_tier,
    last_reading_read_at_plus_30m,
    DATE_TRUNC('WEEK', last_reading_read_at) AS last_reading_read_at_week
FROM scored
WHERE SIZE(quality_list) >= 2
ORDER BY prev_n_quality_not_in_bad_ok
