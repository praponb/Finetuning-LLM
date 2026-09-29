WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ROUND(installed_kw * 0.9, 2) AS installed_kw_adj,
        ADD_MONTHS(commissioned_on, 12) AS commissioned_on_plus_12m,
        SUBSTRING(TRIM(site_name), 1, 4) AS site_name_prefix
    FROM sites
    WHERE tariff = 'EV'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw_adj DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS installed_kw_sum_2,
        ROUND(PERCENT_RANK() OVER w_zone_by_installed_kw_adj_tie, 3) AS installed_kw_adj_percent_rank
    FROM valid
    WINDOW w_zone_by_installed_kw_adj_tie AS (PARTITION BY zone ORDER BY installed_kw_adj DESC)
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.installed_kw,
        s.commissioned_on_plus_12m,
        s.site_name_prefix,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM metrics s
    JOIN meter_readings m ON m.site_id = s.id AND m.kwh > 48.2
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN zone IN ('Z4', 'Z2') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM detail_rows_readings
),
with_parent_reading AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.commissioned_on_plus_12m,
        s.reading_id,
        s.reading_quality,
        s.zone_group,
        m.read_at AS reading_read_at,
        m.site_id AS reading_site_id,
        m.kwh AS reading_kwh_2
    FROM tagged s
    JOIN meter_readings m ON m.id = s.reading_id
),
zone_group_stats AS (
    -- Avg reading_kwh_2 per zone_group
    SELECT zone_group, AVG(reading_kwh_2) AS avg_reading_kwh_2
    FROM with_parent_reading
    GROUP BY zone_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_reading_kwh_2,
        ROUND(s.reading_kwh_2 - g.avg_reading_kwh_2, 2) AS reading_kwh_2_minus_avg
    FROM with_parent_reading s
    JOIN zone_group_stats g ON g.zone_group = s.zone_group
    WHERE s.reading_kwh_2 > g.avg_reading_kwh_2
)
SELECT
    id,
    reading_quality,
    zone_group,
    reading_read_at,
    reading_kwh_2,
    avg_reading_kwh_2,
    SUBSTRING(TRIM(reading_quality), 1, 3) AS reading_quality_prefix,
    CONCAT(reading_quality, '#', CAST(id AS STRING)) AS reading_quality_ref
FROM above_average
WHERE zone_group IN ('group_b', 'group_a')
