WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LENGTH(TRIM(quality)) AS quality_len,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown_2
    FROM meter_readings
    WHERE (site_id >= 7 OR HOUR(read_at) BETWEEN 8 AND 13)
),
combined_details AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_site_id_quality_or_unknown_b, 3) AS voltage_percent_rank,
        ROUND(AVG(site_installed_kw) OVER w_quality_len_site_id, 2) AS quality_len_site_id_avg_site_installed_kw,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality_or_unknown ORDER BY voltage), 3) AS voltage_cume_dist,
        LEAD(site_installed_kw) OVER (PARTITION BY site_id, quality_or_unknown ORDER BY voltage, id) AS next_site_installed_kw,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality_len, site_id), 2) AS kwh_vs_quality_len_site_id_avg
    FROM combined_details
    WINDOW w_site_id_quality_or_unknown_b AS (PARTITION BY site_id, quality_or_unknown ORDER BY voltage), w_quality_len_site_id AS (PARTITION BY quality_len, site_id)
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND MONTH(m.read_at) IN (1, 3)
        )
),
unique_rows AS (
    -- Keep the latest row per quality, quality_or_unknown
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_or_unknown,
        quality_len,
        quality_or_unknown_2,
        site_installed_kw,
        site_commissioned_on,
        voltage_percent_rank,
        quality_len_site_id_avg_site_installed_kw,
        voltage_cume_dist,
        next_site_installed_kw,
        kwh_vs_quality_len_site_id_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, quality_or_unknown ORDER BY site_commissioned_on DESC, id DESC) AS row_num
        FROM with_activity
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    kwh,
    voltage,
    quality,
    quality_or_unknown,
    quality_or_unknown_2,
    site_commissioned_on,
    voltage_cume_dist,
    QUARTER(site_commissioned_on) AS site_commissioned_on_quarter
FROM unique_rows
ORDER BY voltage_cume_dist, quality_or_unknown_2
