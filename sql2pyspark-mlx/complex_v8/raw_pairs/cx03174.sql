WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 216.8 AND 241.8
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone, si.site_name
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff = 'TOU'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_zone) AS rows_in_read_at_month_site_zone,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality, site_zone ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        RANK() OVER (PARTITION BY quality, site_zone ORDER BY voltage) AS voltage_rank,
        SIZE(COLLECT_SET(quality) OVER w_read_at_month_site_zone) AS distinct_quality_in_read_at_month_site_zone,
        LEAD(kwh) OVER (PARTITION BY quality ORDER BY kwh DESC, id) AS next_kwh,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_quality_by_kwh)) / 60, 1) AS minutes_since_prev_read_at,
        NTILE(3) OVER (PARTITION BY quality, site_zone ORDER BY voltage, id) AS voltage_tercile,
        ROUND(AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_zone), 2) AS read_at_month_site_zone_avg_kwh,
        RANK() OVER (PARTITION BY site_zone ORDER BY voltage) AS voltage_rank_2,
        ROUND(kwh - AVG(kwh) OVER w_read_at_month_site_zone, 2) AS kwh_vs_read_at_month_site_zone_avg
    FROM with_parent
    WINDOW w_read_at_month_site_zone AS (PARTITION BY DATE_TRUNC('MONTH', read_at), site_zone), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh DESC, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        site_zone,
        site_name,
        rows_in_read_at_month_site_zone,
        last_known_read_at,
        voltage_rank,
        distinct_quality_in_read_at_month_site_zone,
        next_kwh,
        minutes_since_prev_read_at,
        voltage_tercile,
        read_at_month_site_zone_avg_kwh,
        kwh_vs_read_at_month_site_zone_avg
    FROM metrics
    WHERE voltage_rank_2 <= 3
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    site_name,
    rows_in_read_at_month_site_zone,
    voltage_rank,
    distinct_quality_in_read_at_month_site_zone,
    minutes_since_prev_read_at,
    read_at_month_site_zone_avg_kwh,
    kwh_vs_read_at_month_site_zone_avg,
    DATEDIFF(DATE '2025-12-31', last_known_read_at) AS days_since_last_known_read_at,
    LENGTH(TRIM(quality)) AS quality_len
FROM leaders
ORDER BY minutes_since_prev_read_at
