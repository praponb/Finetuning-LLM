WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE kwh > 35.0
),
combined_details AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id AND si.tariff = 'TOU'
    WHERE si.zone NOT IN ('Z3', 'Z2')
),
site_zone_stats AS (
    -- Avg read_at_month per site_zone
    SELECT site_zone, AVG(read_at_month) AS avg_read_at_month
    FROM combined_details
    GROUP BY site_zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_read_at_month,
        ROUND(s.read_at_month - g.avg_read_at_month, 2) AS read_at_month_minus_avg
    FROM combined_details s
    JOIN site_zone_stats g ON g.site_zone = s.site_zone
    WHERE s.read_at_month > g.avg_read_at_month
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_clean,
        read_at_month_minus_avg,
        ROUND(CUME_DIST() OVER (ORDER BY kwh DESC, id), 3) AS kwh_cume_dist,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY TO_DATE(read_at)), 0), 4) AS kwh_share_of_read_at_to_date,
        MIN(avg_read_at_month) OVER w_read_at_to_date AS read_at_to_date_min_avg_read_at_month
    FROM above_average
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at))
)
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    MIN(voltage) AS min_voltage,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM metrics
GROUP BY GROUPING SETS ((site_id, quality), (site_id), ())
ORDER BY site_id NULLS LAST, quality NULLS LAST
