WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 234.7
),
enriched_site AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone, si.commissioned_on AS site_commissioned_on
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY site_zone ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_sum_6_prior,
        MIN(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        RANK() OVER w_site_zone_by_kwh_ties AS kwh_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY site_zone ORDER BY kwh, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_zone)) AS distinct_site_id_in_site_zone,
        kwh - LAG(kwh) OVER (ORDER BY voltage ASC, id) AS kwh_change,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_zone ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on,
        DENSE_RANK() OVER w_site_zone_by_kwh_ties AS kwh_dense_rank,
        LAG(read_at) OVER w_site_zone_by_kwh AS prev_read_at,
        SUM(kwh) OVER (PARTITION BY site_id) AS site_id_sum_kwh,
        ROW_NUMBER() OVER (ORDER BY voltage ASC, id) AS rn,
        DENSE_RANK() OVER w_site_id_by_voltage_ties AS voltage_dense_rank,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg
    FROM enriched_site
    WINDOW w_site_zone_by_kwh_ties AS (PARTITION BY site_zone ORDER BY kwh), w_site_zone_by_kwh AS (PARTITION BY site_zone ORDER BY kwh, id), w_site_id_by_voltage_ties AS (PARTITION BY site_id ORDER BY voltage ASC)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        LOWER(REGEXP_REPLACE(TRIM(site_zone), ' ', '_')) AS site_zone_slug
    FROM with_trends
    WHERE first_known_site_commissioned_on < DATE '2024-11-21'
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    quality,
    site_zone,
    site_commissioned_on,
    kwh_sum_6_prior,
    min_kwh_so_far,
    kwh_rank,
    voltage_avg_4,
    distinct_site_id_in_site_zone,
    kwh_change,
    rows_in_site_id,
    first_known_site_commissioned_on,
    kwh_dense_rank,
    site_id_sum_kwh,
    rn,
    voltage_dense_rank,
    voltage_vs_site_id_avg,
    read_at_hour,
    site_zone_slug,
    ROUND(voltage_vs_site_id_avg * 1.1, 2) AS voltage_vs_site_id_avg_adj
FROM derived
ORDER BY site_id_sum_kwh DESC, rows_in_site_id DESC
