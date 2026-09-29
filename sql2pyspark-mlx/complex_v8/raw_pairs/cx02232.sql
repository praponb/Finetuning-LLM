WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE kwh BETWEEN 26.9 AND 106.5
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM screened s
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_month,
        s.read_at_week,
        s.reading_count,
        si.installed_kw AS site_installed_kw
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        quality,
        read_at_month,
        read_at_week,
        reading_count,
        ROUND(reading_count - AVG(reading_count) OVER w_site_id_read_at_to_date, 2) AS reading_count_vs_site_id_read_at_to_date_avg,
        FIRST_VALUE(read_at_week) IGNORE NULLS OVER (PARTITION BY YEAR(read_at) ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_week,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, TO_DATE(read_at))) AS distinct_quality_in_site_id_read_at_to_date,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at)), 0), 4) AS voltage_share_of_site_id_read_at_to_date,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at)), 2) AS voltage_vs_site_id_read_at_to_date_avg,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER w_site_id_read_at_to_date, 2) AS site_installed_kw_vs_site_id_read_at_to_date_avg
    FROM combined_details
    WINDOW w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at))
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', read_at_week) AS read_at_week_month,
        TO_DATE(read_at) AS read_at_day,
        DATE_FORMAT(read_at_month, 'yyyy-MM') AS read_at_month_ym,
        COUNT(*) AS cnt,
        ROUND(STDDEV(reading_count), 2) AS stddev_reading_count,
        SUM(CASE WHEN quality IS NOT NULL THEN voltage_vs_site_id_read_at_to_date_avg ELSE 0 END) AS voltage_vs_site_id_read_at_to_date_avg_quality_i,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ROUND(SUM(site_installed_kw_vs_site_id_read_at_to_date_avg), 2) AS total_site_installed_kw_vs_site_id_read_at_to_da
    FROM metrics
    GROUP BY 1, 2, 3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(distinct_qualitys) OVER (PARTITION BY distinct_qualitys, cnt), 2) AS distinct_qualitys_cnt_avg_distinct_qualitys,
        MIN(stddev_reading_count) OVER (PARTITION BY cnt, TO_DATE(read_at_week_month) ORDER BY voltage_vs_site_id_read_at_to_date_avg_quality_i, read_at_week_month, read_at_day, read_at_month_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_stddev_reading_count_so_far,
        ROUND(distinct_qualitys - AVG(distinct_qualitys) OVER (PARTITION BY distinct_qualitys, cnt), 2) AS distinct_qualitys_vs_distinct_qualitys_cnt_avg
    FROM summary
)
-- Final projection
SELECT
    read_at_week_month,
    read_at_month_ym,
    cnt,
    stddev_reading_count,
    distinct_qualitys,
    total_site_installed_kw_vs_site_id_read_at_to_da,
    distinct_qualitys_cnt_avg_distinct_qualitys,
    min_stddev_reading_count_so_far,
    CONCAT(read_at_month_ym, '#', CAST(cnt AS STRING)) AS read_at_month_ym_ref,
    DATE_FORMAT(read_at_day, 'yyyy-MM-dd') AS read_at_day_label
FROM with_trends
ORDER BY read_at_month_ym, min_stddev_reading_count_so_far DESC
