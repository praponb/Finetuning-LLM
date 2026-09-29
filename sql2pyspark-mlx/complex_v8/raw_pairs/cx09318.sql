WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE quality IN ('OK', 'BAD')
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM recent_readings s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw >= 141.6
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        site_id,
        quality,
        DATE_FORMAT(read_at_date, 'yyyy-MM') AS read_at_date_ym,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(AVG(kwh), 2) AS avg_kwh,
        ROUND(STDDEV(kwh), 2) AS stddev_kwh
    FROM qualified
    WHERE site_id <= 9
    GROUP BY site_id, quality, DATE_FORMAT(read_at_date, 'yyyy-MM')
)
SELECT site_id, quality, field, value
FROM (
    SELECT site_id, quality, stddev_kwh, avg_kwh
    FROM rollup_base
) src
UNPIVOT (
    value FOR field IN (stddev_kwh AS stddev_kwh_v, avg_kwh AS avg_kwh_v)
) unp
