WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_ref,
        s.quality_prefix,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        voltage,
        quality,
        quality_prefix,
        site_name,
        site_installed_kw,
        ROW_NUMBER() OVER w_quality_site_id_by_site_comm AS seq_no,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_id), 0), 4) AS site_installed_kw_share_of_site_id,
        MIN(site_installed_kw) OVER w_quality_site_id AS quality_site_id_min_site_installed_kw,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on, id), 3) AS site_commissioned_on_cume_dist,
        NTILE(3) OVER w_site_commissioned_on_month_b_2 AS voltage_tercile,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on, id) AS site_commissioned_on_rank,
        MAX(voltage) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        NTILE(5) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on, id) AS site_commissioned_on_quintile,
        DATEDIFF(site_commissioned_on, LAG(site_commissioned_on) OVER w_quality_site_id_by_site_comm) AS days_since_prev_site_commissioned_on,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        SUM(site_installed_kw) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_site_installed_kw
    FROM with_details_site
    WINDOW w_quality_site_id_by_site_comm AS (PARTITION BY quality, site_id ORDER BY site_commissioned_on, id), w_quality_site_id AS (PARTITION BY quality, site_id), w_site_commissioned_on_month_b_2 AS (PARTITION BY MONTH(site_commissioned_on) ORDER BY voltage DESC, id), w_site_id AS (PARTITION BY site_id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        voltage,
        quality,
        quality_prefix,
        site_name,
        site_installed_kw,
        seq_no,
        first_known_site_commissioned_on,
        site_installed_kw_share_of_site_id,
        quality_site_id_min_site_installed_kw,
        site_commissioned_on_cume_dist,
        voltage_tercile,
        max_voltage_so_far,
        site_commissioned_on_quintile,
        days_since_prev_site_commissioned_on,
        voltage_share_of_quality_site_id,
        quality_site_id_sum_site_installed_kw
    FROM windowed
    WHERE site_commissioned_on_rank = 1
)
-- Final projection
SELECT
    id,
    voltage,
    quality,
    quality_prefix,
    site_installed_kw,
    seq_no,
    first_known_site_commissioned_on,
    site_installed_kw_share_of_site_id,
    quality_site_id_min_site_installed_kw,
    site_commissioned_on_cume_dist,
    voltage_tercile,
    max_voltage_so_far,
    days_since_prev_site_commissioned_on,
    voltage_share_of_quality_site_id,
    quality_site_id_sum_site_installed_kw,
    COALESCE(seq_no, 0) AS seq_no_filled,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
FROM leaders
WHERE site_installed_kw BETWEEN 66.5 AND 373.3
ORDER BY quality DESC
