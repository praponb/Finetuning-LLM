WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(voltage * 0.9, 2) AS voltage_adj
    FROM meter_readings
    WHERE voltage > 222.5
      AND quality NOT IN ('EST', 'BAD')
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM prepared_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage_adj,
        site_commissioned_on,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY site_installed_kw NULLS LAST), 3) AS site_installed_kw_cume_dist,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY kwh, id) AS voltage_change,
        FIRST_VALUE(voltage_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY site_installed_kw NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage_adj,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), YEAR(site_commissioned_on)), 2) AS kwh_vs_read_at_month_site_commissioned_on_year_a,
        MIN(voltage_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on)) AS site_commissioned_on_month_min_voltage_adj,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(PERCENT_RANK() OVER w_all_by_kwh, 3) AS kwh_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS rn,
        SUM(voltage) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh ASC), 3) AS kwh_percent_rank_2,
        NTH_VALUE(site_installed_kw, 2) OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_installed_kw
    FROM with_details_site
    WINDOW w_all_by_kwh AS (ORDER BY kwh ASC, id)
),
derived AS (
    -- Derive helper columns
    SELECT *, YEAR(read_at) AS read_at_year
    FROM windowed
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        voltage,
        voltage_adj,
        site_commissioned_on,
        site_installed_kw_cume_dist,
        voltage_change,
        kwh_vs_read_at_month_site_commissioned_on_year_a,
        site_commissioned_on_month_min_voltage_adj,
        rows_in_site_id,
        kwh_percent_rank,
        running_voltage,
        kwh_percent_rank_2,
        second_site_installed_kw,
        read_at_year,
        ROUND(kwh_percent_rank / NULLIF(SUM(kwh_percent_rank) OVER (PARTITION BY site_id, read_at_year), 0), 4) AS kwh_percent_rank_share_of_site_id_read_at_year,
        MAX(first_voltage_adj) OVER (PARTITION BY site_id, read_at_year) AS site_id_read_at_year_max_first_voltage_adj,
        LAST_VALUE(kwh) OVER (ORDER BY kwh_percent_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        SUM(running_voltage) OVER (PARTITION BY site_id, read_at_year) AS site_id_read_at_year_sum_running_voltage
    FROM derived
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    voltage_adj,
    site_commissioned_on,
    site_installed_kw_cume_dist,
    voltage_change,
    kwh_vs_read_at_month_site_commissioned_on_year_a,
    site_commissioned_on_month_min_voltage_adj,
    rows_in_site_id,
    kwh_percent_rank,
    running_voltage,
    kwh_percent_rank_2,
    second_site_installed_kw,
    site_id_read_at_year_max_first_voltage_adj,
    last_kwh,
    site_id_read_at_year_sum_running_voltage
FROM with_windows
ORDER BY kwh_vs_read_at_month_site_commissioned_on_year_a
