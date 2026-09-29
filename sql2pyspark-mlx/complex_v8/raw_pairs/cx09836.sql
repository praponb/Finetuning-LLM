WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        ROUND(kwh * 0.25, 2) AS kwh_adj
    FROM meter_readings
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_label,
        s.kwh_adj,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        voltage,
        quality,
        read_at_label,
        kwh_adj,
        site_installed_kw,
        LEAD(read_at, 3) OVER (PARTITION BY quality, site_id ORDER BY voltage, id) AS next3_read_at,
        NTILE(4) OVER (PARTITION BY quality ORDER BY site_installed_kw DESC, id) AS site_installed_kw_quartile,
        NTILE(3) OVER (ORDER BY voltage, id) AS voltage_tercile,
        MAX(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        MIN(site_installed_kw) OVER (PARTITION BY site_id) AS site_id_min_site_installed_kw,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        LEAD(read_at) OVER w_quality_site_id_by_voltage AS next_read_at
    FROM joined
    WINDOW w_quality_site_id_by_voltage AS (PARTITION BY quality, site_id ORDER BY voltage, id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(kwh_adj * 1.5, 2) AS kwh_adj_adj,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM with_trends
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(rows_in_site_id) OVER (PARTITION BY voltage_tercile, distinct_quality_in_site_id ORDER BY distinct_quality_in_site_id DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS rows_in_site_id_avg_1_prior,
        SUM(rows_in_site_id) OVER (PARTITION BY voltage_tercile ORDER BY next_read_at ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rows_in_site_id_sum_3_prior,
        FIRST_VALUE(next_read_at) IGNORE NULLS OVER (ORDER BY site_installed_kw_quartile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_read_at
    FROM scored
)
SELECT
    id,
    voltage,
    read_at_label,
    kwh_adj,
    site_installed_kw,
    next3_read_at,
    site_installed_kw_quartile,
    voltage_tercile,
    voltage_max_3,
    site_id_min_site_installed_kw,
    distinct_quality_in_site_id,
    next_read_at,
    quality_prefix,
    rows_in_site_id_avg_1_prior,
    first_known_next_read_at
FROM windowed
ORDER BY id DESC NULLS FIRST, voltage_max_3 DESC
LIMIT 20
