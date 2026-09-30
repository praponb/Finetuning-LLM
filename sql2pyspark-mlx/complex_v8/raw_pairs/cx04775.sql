WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('QUARTER', commissioned_on) AS commissioned_on_quarter,
        UPPER(TRIM(tariff)) AS tariff_clean
    FROM sites
    WHERE zone = 'Z3'
      AND tariff = 'TOU'
),
derived AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label
    FROM base
),
zone_stats AS (
    -- Avg installed_kw per zone
    SELECT zone, AVG(installed_kw) AS avg_installed_kw
    FROM derived
    GROUP BY zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_installed_kw,
        ROUND(s.installed_kw - g.avg_installed_kw, 2) AS installed_kw_minus_avg
    FROM derived s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.installed_kw > g.avg_installed_kw
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        tariff_clean,
        commissioned_on_label,
        installed_kw_minus_avg,
        DAYOFWEEK(commissioned_on_quarter) AS commissioned_on_quarter_dayofweek,
        SUBSTRING(TRIM(site_name), 1, 3) AS site_name_prefix,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM above_average
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.installed_kw,
        s.tariff_clean,
        s.installed_kw_minus_avg,
        s.site_name_prefix,
        s.installed_kw_bucket,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM tagged s
    JOIN meter_readings m ON m.site_id = s.id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        installed_kw,
        tariff_clean,
        installed_kw_minus_avg,
        site_name_prefix,
        installed_kw_bucket,
        reading_id,
        reading_voltage,
        installed_kw - LAG(installed_kw) OVER w_all_by_reading_kwh AS installed_kw_change,
        LAG(installed_kw_minus_avg, 1, 0) OVER (ORDER BY reading_kwh DESC, id, reading_id) AS prev_installed_kw_minus_avg,
        COUNT(*) OVER (PARTITION BY reading_id) AS rows_in_reading_id,
        NTILE(3) OVER (ORDER BY reading_kwh DESC, id, reading_id) AS reading_kwh_tercile,
        ROUND(reading_voltage / NULLIF(SUM(reading_voltage) OVER (PARTITION BY reading_id), 0), 4) AS reading_voltage_share_of_reading_id
    FROM line_level
    WINDOW w_all_by_reading_kwh AS (ORDER BY reading_kwh DESC, id, reading_id)
)
-- Subtotals with GROUPING SETS
SELECT
    reading_kwh_tercile,
    reading_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS row_count,
    COUNT(DISTINCT reading_id) AS distinct_reading_ids
FROM ranked
GROUP BY GROUPING SETS ((reading_kwh_tercile, reading_id), (reading_kwh_tercile))
ORDER BY reading_kwh_tercile NULLS LAST, reading_id NULLS LAST
