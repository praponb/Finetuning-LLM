WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM scoped s
)
SELECT
    quality,
    site_id,
    GROUPING_ID() AS gid,
    MIN(read_at) AS first_read_at,
    ROUND(SUM(reading_count) / NULLIF(SUM(total_reading_kwh), 0), 3) AS reading_count_to_total_reading_kwh_ratio
FROM summarized_rows
GROUP BY CUBE(quality, site_id)
ORDER BY quality NULLS LAST, site_id NULLS LAST
