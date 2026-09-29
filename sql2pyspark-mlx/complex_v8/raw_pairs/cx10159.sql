WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE quality = 'EST'
),
with_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(read_at_dayofweek) OVER (PARTITION BY YEAR(site_commissioned_on), site_id ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS read_at_dayofweek_avg_4,
        FIRST_VALUE(site_installed_kw) IGNORE NULLS OVER (PARTITION BY YEAR(site_commissioned_on), site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_installed_kw,
        RANK() OVER (PARTITION BY YEAR(site_commissioned_on), quality ORDER BY read_at_dayofweek DESC) AS read_at_dayofweek_rank,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY quality, site_id), 2) AS site_installed_kw_vs_quality_site_id_avg,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(CUME_DIST() OVER w_quality_by_site_commissioned_2, 3) AS site_commissioned_on_cume_dist,
        LEAD(read_at_dayofweek) OVER w_site_commissioned_on_year_qu_2 AS next_read_at_dayofweek,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY YEAR(site_commissioned_on), site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        DENSE_RANK() OVER w_quality_by_site_commissioned_3 AS site_commissioned_on_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(site_commissioned_on), quality ORDER BY read_at_dayofweek DESC), 3) AS read_at_dayofweek_percent_rank,
        ROUND(voltage_plus_kwh - AVG(voltage_plus_kwh) OVER w_quality_site_id, 2) AS voltage_plus_kwh_vs_quality_site_id_avg,
        LAG(voltage, 1, 0) OVER w_quality_by_site_commissioned_2 AS prev_voltage,
        FIRST_VALUE(site_installed_kw) OVER (PARTITION BY quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_installed_kw,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        MIN(read_at_dayofweek) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_read_at_dayofweek
    FROM with_details_site
    WINDOW w_quality_by_site_commissioned_2 AS (PARTITION BY quality ORDER BY site_commissioned_on DESC, id), w_site_commissioned_on_year_qu_2 AS (PARTITION BY YEAR(site_commissioned_on), quality ORDER BY read_at_dayofweek DESC, id), w_quality_by_site_commissioned_3 AS (PARTITION BY quality ORDER BY site_commissioned_on DESC), w_quality_site_id AS (PARTITION BY quality, site_id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        quality,
        read_at_dayofweek,
        site_commissioned_on,
        site_name,
        site_installed_kw,
        first_known_site_installed_kw,
        site_installed_kw_vs_quality_site_id_avg,
        second_read_at,
        site_commissioned_on_dense_rank,
        read_at_dayofweek_percent_rank,
        voltage_plus_kwh_vs_quality_site_id_avg,
        prev_voltage,
        voltage_share_of_quality_site_id,
        COUNT(*) OVER w_site_commissioned_on_dense_r AS rows_in_site_commissioned_on_dense_rank_second_r,
        ROUND(prev_voltage - AVG(prev_voltage) OVER w_site_commissioned_on_dense_r, 2) AS prev_voltage_vs_site_commissioned_on_dense_rank_,
        SUM(next_read_at_dayofweek) OVER (PARTITION BY site_commissioned_on_dense_rank, YEAR(second_read_at)) AS site_commissioned_on_dense_rank_second_read_at_y,
        DENSE_RANK() OVER w_site_commissioned_on_month_b AS first_site_installed_kw_dense_rank
    FROM metrics
    WINDOW w_site_commissioned_on_dense_r AS (PARTITION BY site_commissioned_on_dense_rank, YEAR(second_read_at)), w_site_commissioned_on_month_b AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY first_site_installed_kw DESC)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    read_at_dayofweek,
    site_commissioned_on,
    site_name,
    site_installed_kw,
    first_known_site_installed_kw,
    site_installed_kw_vs_quality_site_id_avg,
    site_commissioned_on_dense_rank,
    read_at_dayofweek_percent_rank,
    voltage_plus_kwh_vs_quality_site_id_avg,
    prev_voltage,
    prev_voltage_vs_site_commissioned_on_dense_rank_,
    site_commissioned_on_dense_rank_second_read_at_y,
    first_site_installed_kw_dense_rank,
    GREATEST(prev_voltage, site_installed_kw) AS greatest_prev_voltage_site_installed_kw,
    TO_DATE(read_at) AS read_at_date
FROM with_trends
