WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(kwh - 81.9) AS kwh_distance,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE (site_id >= 2 OR read_at >= TIMESTAMP '2025-01-19 09:00:00')
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.read_at_year,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(site_tariff)) AS site_tariff_len,
        COALESCE(kwh, 0.0) AS kwh_filled,
        SUBSTRING(TRIM(site_tariff), 1, 3) AS site_tariff_prefix
    FROM combined_details_site
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        read_at_year,
        site_commissioned_on,
        site_installed_kw,
        site_tariff_len,
        kwh_filled,
        ROUND(CUME_DIST() OVER w_quality_by_site_installed_kw, 3) AS site_installed_kw_cume_dist,
        MAX(kwh_distance) OVER (PARTITION BY YEAR(site_commissioned_on), TO_DATE(read_at_year)) AS site_commissioned_on_year_read_at_year_to_date_m,
        COUNT(*) OVER w_site_commissioned_on_year_re AS rows_in_site_commissioned_on_year_read_at_year_t,
        ROUND(CUME_DIST() OVER (ORDER BY read_at_year DESC), 3) AS read_at_year_cume_dist
    FROM annotated
    WINDOW w_quality_by_site_installed_kw AS (PARTITION BY quality ORDER BY site_installed_kw), w_site_commissioned_on_year_re AS (PARTITION BY YEAR(site_commissioned_on), TO_DATE(read_at_year))
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(site_commissioned_on, 'yyyy-MM') AS site_commissioned_on_label
    FROM with_trends
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    read_at_year,
    site_installed_kw,
    kwh_filled,
    site_installed_kw_cume_dist,
    site_commissioned_on_year_read_at_year_to_date_m,
    rows_in_site_commissioned_on_year_read_at_year_t,
    read_at_year_cume_dist,
    site_commissioned_on_label
FROM enriched
WHERE site_tariff_len BETWEEN 3 AND 11
