WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE voltage < 233.6
      AND HOUR(read_at) BETWEEN 10 AND 14
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_year,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.zone AS site_zone
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_year,
        site_installed_kw,
        site_zone,
        NTH_VALUE(read_at_year, 2) OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_year,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at_year ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_avg_4,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER w_site_id, 2) AS site_installed_kw_vs_site_id_avg,
        LEAD(read_at, 3) OVER (PARTITION BY site_zone, quality ORDER BY read_at, id) AS next3_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY site_zone, quality ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        SIZE(COLLECT_SET(site_id) OVER w_read_at_year_month) AS distinct_site_id_in_read_at_year_month,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY MONTH(read_at_year)), 2) AS voltage_vs_read_at_year_month_avg,
        NTILE(3) OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at_year ASC, id) AS read_at_year_tercile
    FROM with_parent
    WINDOW w_site_id AS (PARTITION BY site_id), w_read_at_year_month AS (PARTITION BY MONTH(read_at_year))
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY site_id, read_at_year_tercile ORDER BY read_at_year NULLS LAST) AS read_at_year_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage NULLS LAST, id), 3) AS voltage_percent_rank,
        MAX(voltage) OVER (PARTITION BY site_zone ORDER BY kwh_avg_4, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        ROUND((UNIX_TIMESTAMP(read_at_year) - UNIX_TIMESTAMP(LAG(read_at_year) OVER (PARTITION BY site_id, read_at_year_tercile ORDER BY read_at_year NULLS LAST, id))) / 60, 1) AS minutes_since_prev_read_at_year,
        ROUND(AVG(site_installed_kw) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_avg_4_2,
        DENSE_RANK() OVER w_all_by_kwh_avg_4_ties AS kwh_avg_4_dense_rank
    FROM metrics
    WINDOW w_all_by_kwh_avg_4_ties AS (ORDER BY kwh_avg_4 DESC)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    read_at_year,
    site_installed_kw,
    site_installed_kw_avg_4,
    site_installed_kw_vs_site_id_avg,
    distinct_site_id_in_read_at_year_month,
    voltage_vs_read_at_year_month_avg,
    read_at_year_tercile,
    voltage_percent_rank,
    voltage_max_3,
    minutes_since_prev_read_at_year,
    site_installed_kw_avg_4_2,
    kwh_avg_4_dense_rank,
    SUBSTRING(TRIM(site_zone), 1, 2) AS site_zone_prefix,
    DAYOFWEEK(read_at_year) AS read_at_year_dayofweek
FROM metrics_2
WHERE read_at_year < TIMESTAMP '2025-02-17 09:00:00'
ORDER BY quality
