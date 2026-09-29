WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE voltage <= 232.0
      AND kwh >= 52.4
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_year,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM cleaned s
    JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_tariff)) AS distinct_quality_in_site_tariff,
        kwh - LAG(kwh) OVER (ORDER BY read_at_year ASC, id) AS kwh_change,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_tariff), 2) AS voltage_vs_site_tariff_avg,
        LAG(read_at_year) OVER (ORDER BY read_at_year ASC, id) AS prev_read_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff ORDER BY read_at_year, id), 3) AS read_at_year_cume_dist,
        LAG(voltage, 3) OVER (ORDER BY read_at_year ASC, id) AS prev3_voltage,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_tariff ORDER BY read_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        DENSE_RANK() OVER w_site_tariff_by_read_at_year AS read_at_year_dense_rank,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_year, 3) AS read_at_year_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY read_at_year ASC), 3) AS read_at_year_cume_dist_2,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST, id) AS kwh_rank,
        ROUND(read_at_year - AVG(read_at_year) OVER (PARTITION BY site_tariff), 2) AS read_at_year_vs_site_tariff_avg,
        MAX(voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST), 3) AS kwh_cume_dist,
        LEAD(read_at_year) OVER w_quality_site_id_by_kwh AS next_read_at_year
    FROM combined_details_site
    WINDOW w_site_tariff_by_read_at_year AS (PARTITION BY site_tariff ORDER BY read_at_year, id), w_all_by_read_at_year AS (ORDER BY read_at_year ASC, id), w_quality_site_id_by_kwh AS (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_year,
        site_commissioned_on,
        site_tariff,
        site_installed_kw,
        distinct_quality_in_site_tariff,
        kwh_change,
        voltage_vs_site_tariff_avg,
        prev_read_at_year,
        read_at_year_cume_dist,
        prev3_voltage,
        second_read_at,
        read_at_year_dense_rank,
        second_kwh,
        read_at_year_percent_rank,
        read_at_year_cume_dist_2,
        read_at_year_vs_site_tariff_avg,
        max_voltage_so_far,
        kwh_cume_dist,
        next_read_at_year
    FROM sequenced
    WHERE kwh_rank = 1
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_year,
        site_installed_kw,
        kwh_change,
        voltage_vs_site_tariff_avg,
        prev_read_at_year,
        read_at_year_cume_dist,
        second_read_at,
        read_at_year_dense_rank,
        second_kwh,
        read_at_year_percent_rank,
        read_at_year_vs_site_tariff_avg,
        max_voltage_so_far,
        kwh_cume_dist,
        SUM(prev_read_at_year) OVER (PARTITION BY read_at_year, site_tariff ORDER BY kwh_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_read_at_year,
        ROUND(kwh_change / NULLIF(SUM(kwh_change) OVER (PARTITION BY quality, prev_read_at_year), 0), 4) AS kwh_change_share_of_quality_prev_read_at_year,
        RANK() OVER (PARTITION BY read_at_year, site_tariff ORDER BY kwh_cume_dist DESC) AS kwh_cume_dist_rank,
        ROUND(read_at_year - AVG(read_at_year) OVER (PARTITION BY quality, prev_read_at_year), 2) AS read_at_year_vs_quality_prev_read_at_year_avg
    FROM top_rows
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    read_at_year,
    kwh_change,
    voltage_vs_site_tariff_avg,
    read_at_year_cume_dist,
    second_read_at,
    read_at_year_dense_rank,
    second_kwh,
    read_at_year_percent_rank,
    read_at_year_vs_site_tariff_avg,
    max_voltage_so_far,
    kwh_cume_dist,
    kwh_change_share_of_quality_prev_read_at_year,
    kwh_cume_dist_rank
FROM sequenced_2
