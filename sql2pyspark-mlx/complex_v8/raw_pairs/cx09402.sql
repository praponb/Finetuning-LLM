WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM meter_readings
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_label,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'TOU'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        voltage,
        site_tariff,
        NTILE(4) OVER (PARTITION BY quality ORDER BY site_installed_kw DESC NULLS FIRST, id) AS site_installed_kw_quartile,
        MAX(voltage) OVER (PARTITION BY quality, site_tariff) AS quality_site_tariff_max_voltage,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY quality, site_tariff), 2) AS site_installed_kw_vs_quality_site_tariff_avg,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY YEAR(read_at), site_id), 0), 4) AS site_installed_kw_share_of_read_at_year_site_id,
        SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        LAG(read_at) OVER w_read_at_month_by_site_instal_2 AS prev_read_at,
        MAX(site_installed_kw) OVER w_read_at_year_site_id AS read_at_year_site_id_max_site_installed_kw,
        ROW_NUMBER() OVER w_site_id_site_tariff_by_site AS seq_no,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id
    FROM with_parent_site
    WINDOW w_read_at_month_by_site_instal_2 AS (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY site_installed_kw DESC, id), w_read_at_year_site_id AS (PARTITION BY YEAR(read_at), site_id), w_site_id_site_tariff_by_site AS (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw DESC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(site_tariff, '#', CAST(seq_no AS STRING)) AS site_tariff_ref,
        ABS(voltage_avg_3 - 227.5) AS voltage_avg_3_distance
    FROM ranked
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    site_installed_kw_quartile,
    site_installed_kw_vs_quality_site_tariff_avg,
    running_voltage,
    prev_read_at,
    read_at_year_site_id_max_site_installed_kw,
    seq_no,
    voltage_avg_3,
    site_tariff_ref,
    voltage_avg_3_distance,
    CASE WHEN site_tariff IN ('FLAT', 'EV') THEN 'group_a' WHEN site_tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS site_tariff_group,
    SUBSTRING(TRIM(site_tariff_ref), 1, 2) AS site_tariff_ref_prefix
FROM tagged
WHERE site_installed_kw_share_of_read_at_year_site_id > 0.2
ORDER BY read_at DESC, site_installed_kw_quartile
