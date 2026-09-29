WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter,
        ABS(kwh - 68.1) AS kwh_distance,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
    WHERE kwh <= 84.9
      AND site_id >= 4
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_quarter,
        s.kwh_distance,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM scoped s
    JOIN sites si ON si.id = s.site_id AND si.zone IN ('Z2', 'Z4')
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM joined_site s
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(first_reading_read_at) AS first_reading_read_at_year,
        ARRAY_SORT(COLLECT_SET(site_zone)) AS site_zone_list,
        SUM(CASE WHEN quality IS NOT NULL THEN 1 ELSE 0 END) AS n_quality_is_not_null,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        SUM(kwh_distance) FILTER (WHERE UPPER(TRIM(site_name)) LIKE 'M%') AS kwh_distance_upper_trim_site_name_like_m,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        COUNT(*) AS n_rows
    FROM with_stats
    WHERE HOUR(read_at_quarter) BETWEEN 9 AND 15
    GROUP BY 1
    HAVING COUNT(*) >= 2
)
SELECT first_reading_read_at_year, metric, metric_value
FROM (
    SELECT first_reading_read_at_year, n_quality_is_not_null, distinct_qualitys
    FROM totals
) src
UNPIVOT (
    metric_value FOR metric IN (n_quality_is_not_null, distinct_qualitys)
) unp
