WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND voltage > 216.1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        quality,
        NTILE(3) OVER w_site_id_quality_by_voltage AS voltage_tercile,
        MAX(kwh) OVER w_quality AS quality_max_kwh,
        SIZE(COLLECT_SET(site_id) OVER w_quality) AS distinct_site_id_in_quality,
        SUM(kwh) OVER (PARTITION BY site_id, quality ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_sum_3,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        MAX(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far
    FROM scoped
    WINDOW w_site_id_quality_by_voltage AS (PARTITION BY site_id, quality ORDER BY voltage DESC NULLS FIRST, id), w_quality AS (PARTITION BY quality)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        quality,
        voltage_tercile,
        quality_max_kwh,
        kwh_sum_3,
        last_known_voltage,
        max_kwh_so_far,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM windowed
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        quality,
        last_known_voltage,
        quality_slug,
        COUNT(*) OVER (PARTITION BY voltage_tercile, quality) AS rows_in_voltage_tercile_quality,
        FIRST_VALUE(max_kwh_so_far) IGNORE NULLS OVER (PARTITION BY quality, quality_group ORDER BY voltage_tercile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_kwh_so_far,
        ROUND(last_known_voltage - AVG(last_known_voltage) OVER (PARTITION BY voltage_tercile, quality), 2) AS last_known_voltage_vs_voltage_tercile_quality_av,
        NTILE(4) OVER (PARTITION BY quality, quality_group ORDER BY voltage_tercile ASC, id) AS voltage_tercile_quartile,
        LEAD(max_kwh_so_far) OVER (PARTITION BY quality, quality_group ORDER BY voltage_tercile ASC, id) AS next_max_kwh_so_far,
        MIN(kwh_sum_3) OVER (PARTITION BY voltage_tercile, quality) AS voltage_tercile_quality_min_kwh_sum_3
    FROM scored
)
SELECT
    id,
    quality,
    last_known_voltage,
    quality_slug,
    rows_in_voltage_tercile_quality,
    last_known_voltage_vs_voltage_tercile_quality_av,
    voltage_tercile_quartile,
    next_max_kwh_so_far,
    voltage_tercile_quality_min_kwh_sum_3,
    next_max_kwh_so_far + last_known_voltage AS next_max_kwh_so_far_plus_last_known_voltage,
    CONCAT_WS(': ', quality, quality_slug) AS quality_quality_slug_label
FROM with_trends
WHERE last_known_voltage BETWEEN 220.2 AND 240.2
ORDER BY voltage_tercile_quartile NULLS LAST, last_known_voltage, id
LIMIT 5
