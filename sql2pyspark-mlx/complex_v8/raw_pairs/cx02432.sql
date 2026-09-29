WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND voltage >= 222.1
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.greatest_kwh_voltage,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        greatest_kwh_voltage,
        site_installed_kw,
        site_commissioned_on,
        DATE_FORMAT(site_commissioned_on, 'yyyy-MM') AS site_commissioned_on_label,
        site_installed_kw - kwh AS site_installed_kw_minus_kwh,
        TO_DATE(read_at) AS read_at_date
    FROM enriched
    WHERE greatest_kwh_voltage <= 208.2
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw_2,
        si.zone AS site_zone
    FROM labeled s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        site_installed_kw,
        site_commissioned_on_label,
        site_installed_kw_minus_kwh,
        read_at_date,
        site_tariff,
        site_installed_kw_2,
        site_zone,
        MAX(site_installed_kw) OVER (PARTITION BY YEAR(read_at_date), site_tariff ORDER BY site_installed_kw_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_site_installed_kw_so_far,
        RANK() OVER (PARTITION BY YEAR(read_at_date), site_tariff ORDER BY site_installed_kw_2 DESC, id) AS site_installed_kw_2_rank,
        ROUND(site_installed_kw_minus_kwh / NULLIF(SUM(site_installed_kw_minus_kwh) OVER w_site_commissioned_on_year, 0), 4) AS site_installed_kw_minus_kwh_share_of_site_commis,
        ROUND(greatest_kwh_voltage / NULLIF(SUM(greatest_kwh_voltage) OVER w_site_commissioned_on_year, 0), 4) AS greatest_kwh_voltage_share_of_site_commissioned_,
        ROUND(AVG(site_installed_kw_minus_kwh) OVER (PARTITION BY YEAR(site_commissioned_on)), 2) AS site_commissioned_on_year_avg_site_installed_kw_,
        NTILE(5) OVER (PARTITION BY YEAR(read_at_date), site_tariff ORDER BY site_installed_kw_2 DESC, id) AS site_installed_kw_2_quintile
    FROM joined
    WINDOW w_site_commissioned_on_year AS (PARTITION BY YEAR(site_commissioned_on))
)
-- Final projection
SELECT
    id,
    read_at,
    site_commissioned_on_label,
    site_installed_kw_minus_kwh,
    read_at_date,
    site_tariff,
    greatest_kwh_voltage_share_of_site_commissioned_,
    site_commissioned_on_year_avg_site_installed_kw_,
    site_installed_kw_2_quintile,
    CONCAT(site_zone, '#', CAST(site_installed_kw_2_rank AS STRING)) AS site_zone_ref
FROM windowed
