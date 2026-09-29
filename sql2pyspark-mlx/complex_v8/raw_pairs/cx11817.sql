WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 218.4
      AND kwh >= 50.4
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        site_tariff,
        site_installed_kw,
        FIRST_VALUE(site_installed_kw) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_installed_kw,
        SUM(site_installed_kw) OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS site_installed_kw_sum_2_prior,
        LAST_VALUE(voltage) OVER (ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY voltage DESC, id) AS voltage_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_site_tariff, 0), 4) AS voltage_share_of_site_id_site_tariff,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_site_id_site_tariff, 0), 4) AS site_installed_kw_share_of_site_id_site_tariff,
        LEAD(site_installed_kw, 2, 0) OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id) AS next2_site_installed_kw,
        ROUND(AVG(kwh) OVER (PARTITION BY YEAR(read_at), site_tariff), 2) AS read_at_year_site_tariff_avg_kwh,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY YEAR(read_at), site_tariff), 2) AS voltage_vs_read_at_year_site_tariff_avg
    FROM enriched
    WINDOW w_site_id_site_tariff AS (PARTITION BY site_id, site_tariff)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY voltage_rank) AS rows_in_voltage_rank,
        FIRST_VALUE(last_voltage) OVER (ORDER BY last_voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_voltage,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY voltage_rank), 2) AS site_installed_kw_vs_voltage_rank_avg
    FROM sequenced
),
summary AS (
    -- Group-level totals
    SELECT
        YEAR(read_at) AS read_at_year,
        SUM(site_installed_kw_vs_voltage_rank_avg) AS total_site_installed_kw_vs_voltage_rank_avg,
        ROUND(AVG(site_installed_kw_vs_voltage_rank_avg), 2) AS avg_site_installed_kw_vs_voltage_rank_avg,
        ROUND(100 * COUNT(CASE WHEN site_tariff = 'TOU' THEN 1 END) / COUNT(*), 1) AS pct_site_tariff_tou,
        ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list
    FROM metrics
    GROUP BY YEAR(read_at)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(read_at_year) OVER (PARTITION BY read_at_year) AS read_at_year_max_read_at_year,
        MAX(pct_site_tariff_tou) OVER (ORDER BY pct_site_tariff_tou DESC, read_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_pct_site_tariff_tou_so_far
    FROM summary
)
SELECT
    read_at_year,
    avg_site_installed_kw_vs_voltage_rank_avg,
    pct_site_tariff_tou,
    read_at_year_max_read_at_year,
    max_pct_site_tariff_tou_so_far
FROM sequenced_2
WHERE avg_site_installed_kw_vs_voltage_rank_avg <= 250.0
ORDER BY max_pct_site_tariff_tou_so_far DESC, read_at_year_max_read_at_year
