WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM valid s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on >= DATE '2024-06-27'
              AND si.zone NOT IN ('Z3', 'Z4')
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        reading_count,
        reading_count_2,
        SIZE(COLLECT_SET(quality) OVER w_reading_count_site_id) AS distinct_quality_in_reading_count_site_id,
        COUNT(*) OVER (PARTITION BY reading_count, site_id) AS rows_in_reading_count_site_id,
        MAX(voltage) OVER w_reading_count_site_id AS reading_count_site_id_max_voltage,
        NTILE(5) OVER (ORDER BY reading_count, id) AS reading_count_quintile
    FROM with_activity
    WINDOW w_reading_count_site_id AS (PARTITION BY reading_count, site_id)
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count,
        COUNT(*) AS reading_count_3
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count_3, 0) AS reading_count_3
    FROM metrics s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.reading_count,
        s.distinct_quality_in_reading_count_site_id,
        s.rows_in_reading_count_site_id,
        s.reading_count_site_id_max_voltage,
        s.reading_count_quintile,
        s.first_reading_read_at,
        s.reading_quality_count,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM summarized_rows s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw <= 284.0
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        reading_count,
        reading_count_quintile,
        YEAR(first_reading_read_at) AS first_reading_read_at_year,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        MAX(first_reading_read_at) AS last_first_reading_read_at,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        SUM(CASE WHEN UPPER(TRIM(site_name)) LIKE 'H%' THEN reading_count ELSE 0 END) AS reading_count_upper_trim_site_name_like_h
    FROM with_details_site
    GROUP BY 1, 2, 3
)
SELECT
    reading_count,
    reading_count_quintile,
    site_name_list,
    last_first_reading_read_at,
    distinct_site_ids,
    reading_count_upper_trim_site_name_like_h,
    SIZE(site_name_list) AS site_name_list_count,
    CASE WHEN reading_count >= 6 THEN 'A' WHEN reading_count >= 2 THEN 'B' ELSE 'C' END AS reading_count_tier
FROM totals
ORDER BY reading_count_quintile
