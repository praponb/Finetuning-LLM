WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label_2,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 9 AND 14 OR voltage <= 238.8)
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_label_2,
        si.site_name,
        si.tariff AS site_tariff
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        RANK() OVER (PARTITION BY quality, site_tariff ORDER BY voltage ASC) AS voltage_rank,
        SUM(voltage) OVER (PARTITION BY quality, site_tariff ORDER BY voltage ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_sum_4,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality, site_tariff ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at
    FROM combined_details
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_label_2,
        s.site_name,
        s.site_tariff,
        s.voltage_vs_site_id_avg,
        s.voltage_rank,
        s.voltage_sum_4,
        s.last_known_read_at,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff_2,
        si.installed_kw AS site_installed_kw
    FROM windowed s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT id, metric_name, amount
FROM (
    SELECT id, site_installed_kw, voltage
    FROM with_details
) src
UNPIVOT (
    amount FOR metric_name IN (site_installed_kw AS m_site_installed_kw, voltage AS voltage_v)
) unp
