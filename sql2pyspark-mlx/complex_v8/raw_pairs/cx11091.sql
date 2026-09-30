WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'BAD')
      AND kwh >= 14.4
),
combined_details AS (
    -- Join sites
    SELECT s.*, si.site_name
    FROM filtered_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.installed_kw <= 373.3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY TO_DATE(read_at), quality_or_unknown)) AS distinct_quality_in_read_at_to_date_quality_or_u,
        ROUND(AVG(voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        MAX(voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_max_1_prior,
        LAG(voltage, 2) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id) AS prev2_voltage,
        LAG(voltage, 2) OVER w_all_by_kwh AS prev2_voltage_2,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id))) / 60, 1) AS minutes_since_prev_read_at
    FROM combined_details
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_name,
    voltage_avg_4,
    first_known_voltage,
    voltage_max_1_prior,
    prev2_voltage_2,
    minutes_since_prev_read_at
FROM windowed
ORDER BY id DESC, site_id DESC
