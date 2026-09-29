WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE quality IN ('BAD', 'OK')
      AND HOUR(read_at) BETWEEN 10 AND 12
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        site_installed_kw - LAG(site_installed_kw) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC, id) AS site_installed_kw_change,
        LEAD(site_commissioned_on) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY site_installed_kw DESC NULLS FIRST, id) AS next_site_commissioned_on,
        COUNT(*) OVER w_read_at_to_date AS rows_in_read_at_to_date,
        ROUND(AVG(site_installed_kw) OVER w_site_id, 2) AS site_id_avg_site_installed_kw,
        kwh - LAG(kwh) OVER w_site_commissioned_on_year_si AS kwh_change,
        MIN(site_installed_kw) OVER w_read_at_to_date AS read_at_to_date_min_site_installed_kw,
        LAG(read_at) OVER w_site_id_quality_by_kwh AS prev_read_at,
        MIN(kwh) OVER (PARTITION BY TO_DATE(read_at)) AS read_at_to_date_min_kwh
    FROM enriched_site
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at)), w_site_id AS (PARTITION BY site_id), w_site_commissioned_on_year_si AS (PARTITION BY YEAR(site_commissioned_on), site_id ORDER BY site_installed_kw DESC, id), w_site_id_quality_by_kwh AS (PARTITION BY site_id, quality ORDER BY kwh ASC, id)
),
scored AS (
    -- Label and score rows
    SELECT *, kwh_change - kwh AS kwh_change_minus_kwh
    FROM ranked
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    site_installed_kw_change,
    next_site_commissioned_on,
    rows_in_read_at_to_date,
    site_id_avg_site_installed_kw,
    kwh_change,
    read_at_to_date_min_site_installed_kw,
    read_at_to_date_min_kwh
FROM scored
ORDER BY site_id DESC, id
LIMIT 5
