WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE kwh < 109.3
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.days_since_read_at,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(site_installed_kw) OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_2,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY YEAR(site_commissioned_on)), 2) AS voltage_vs_site_commissioned_on_year_avg,
        LAG(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id) AS prev_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_commissioned_on_year, 0), 4) AS voltage_share_of_site_commissioned_on_year,
        MAX(days_since_read_at) OVER (ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_read_at_so_far,
        SIZE(COLLECT_SET(site_id) OVER w_quality_read_at_year) AS distinct_site_id_in_quality_read_at_year,
        LAST_VALUE(site_installed_kw) OVER (ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        MAX(site_installed_kw) OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_site_installed_kw_so_far
    FROM with_details
    WINDOW w_site_commissioned_on_year AS (PARTITION BY YEAR(site_commissioned_on)), w_quality_read_at_year AS (PARTITION BY quality, YEAR(read_at))
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        days_since_read_at,
        site_commissioned_on,
        last_known_voltage,
        voltage_vs_site_commissioned_on_year_avg,
        prev_voltage,
        voltage_share_of_site_commissioned_on_year,
        distinct_site_id_in_quality_read_at_year,
        last_site_installed_kw,
        second_voltage,
        max_site_installed_kw_so_far,
        COUNT(*) OVER (PARTITION BY distinct_site_id_in_quality_read_at_year, site_id) AS rows_in_distinct_site_id_in_quality_read_at_year,
        ROUND(AVG(prev_voltage) OVER (PARTITION BY site_id, quality ORDER BY voltage_share_of_site_commissioned_on_year, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS prev_voltage_avg_2,
        ROUND(AVG(site_installed_kw_max_2) OVER (PARTITION BY site_id, quality ORDER BY voltage_share_of_site_commissioned_on_year, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_max_2_avg_2,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY distinct_site_id_in_quality_read_at_year, site_id), 2) AS voltage_vs_distinct_site_id_in_quality_read_at_y
    FROM ranked
),
summary AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at) AS read_at_day,
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        MIN(voltage_share_of_site_commissioned_on_year) AS min_voltage_share_of_site_commissioned_on_year,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(last_known_voltage), 2) AS total_last_known_voltage
    FROM sequenced
    GROUP BY TO_DATE(read_at), DATE_TRUNC('MONTH', site_commissioned_on)
)
SELECT
    read_at_day,
    min_voltage_share_of_site_commissioned_on_year,
    total_last_known_voltage
FROM summary
ORDER BY read_at_day DESC, total_last_known_voltage NULLS LAST
