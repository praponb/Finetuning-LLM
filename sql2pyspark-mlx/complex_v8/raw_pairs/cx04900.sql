WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 80.8 THEN 'A' WHEN kwh >= 43.1 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE quality IN ('OK', 'EST')
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.kwh_tier,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(read_at) OVER (PARTITION BY kwh_tier ORDER BY read_at DESC, id) AS prev_read_at,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY YEAR(site_commissioned_on)), 2) AS voltage_vs_site_commissioned_on_year_avg,
        LAG(read_at, 3) OVER w_all_by_read_at AS prev3_read_at,
        COUNT(*) OVER w_read_at_month AS rows_in_read_at_month,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY MONTH(read_at))) AS distinct_site_zone_in_read_at_month,
        LAG(voltage) OVER w_all_by_site_commissioned_on AS prev_voltage,
        MIN(voltage) OVER (PARTITION BY MONTH(read_at)) AS read_at_month_min_voltage,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY kwh_tier ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on
    FROM combined_details
    WINDOW w_all_by_read_at AS (ORDER BY read_at ASC, id), w_read_at_month AS (PARTITION BY MONTH(read_at)), w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on DESC NULLS FIRST, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', kwh_tier, site_zone) AS kwh_tier_site_zone_label,
        LOWER(REGEXP_REPLACE(TRIM(kwh_tier), ' ', '_')) AS kwh_tier_slug
    FROM ranked
    WHERE site_id >= 5
)
SELECT
    id,
    site_id,
    read_at,
    kwh_tier,
    site_zone,
    prev_read_at,
    voltage_vs_site_commissioned_on_year_avg,
    rows_in_read_at_month,
    distinct_site_zone_in_read_at_month,
    prev_voltage,
    second_site_commissioned_on
FROM labeled
