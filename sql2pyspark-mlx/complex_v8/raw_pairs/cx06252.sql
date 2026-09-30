WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM cleaned s
    JOIN sites si ON si.id = s.site_id AND UPPER(TRIM(si.site_name)) LIKE 'S%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id) AS next_kwh,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        LAST_VALUE(site_installed_kw) OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        RANK() OVER (ORDER BY site_installed_kw) AS site_installed_kw_rank,
        SUM(kwh) OVER (PARTITION BY site_id) AS site_id_sum_kwh,
        LEAD(voltage) OVER (ORDER BY site_installed_kw, id) AS next_voltage,
        LAG(kwh, 1, 0) OVER (ORDER BY site_installed_kw, id) AS prev_kwh,
        FIRST_VALUE(voltage) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        kwh - LAG(kwh) OVER (ORDER BY site_installed_kw, id) AS kwh_change,
        LEAD(kwh, 3) OVER (PARTITION BY site_id ORDER BY kwh, id) AS next3_kwh,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        LAG(voltage) OVER (ORDER BY site_installed_kw, id) AS prev_voltage
    FROM combined_details_site
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    site_installed_kw,
    site_name,
    next_kwh,
    rows_in_site_id,
    last_site_installed_kw,
    site_installed_kw_rank,
    site_id_sum_kwh,
    next_voltage,
    prev_kwh,
    first_voltage,
    kwh_change,
    next3_kwh,
    first_known_voltage,
    prev_voltage,
    CONCAT(site_name, '#', CAST(rows_in_site_id AS STRING)) AS site_name_ref,
    COALESCE(site_id_sum_kwh, 0.0) AS site_id_sum_kwh_filled
FROM sequenced
ORDER BY rows_in_site_id DESC, site_name_ref
