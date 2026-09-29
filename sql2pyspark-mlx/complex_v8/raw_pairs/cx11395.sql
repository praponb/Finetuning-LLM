WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality = 'EST'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER w_all_by_read_at AS read_at_tercile,
        SUM(kwh) OVER w_site_id_quality AS site_id_quality_sum_kwh,
        ROUND(kwh_minus_voltage - AVG(kwh_minus_voltage) OVER (PARTITION BY site_id, quality), 2) AS kwh_minus_voltage_vs_site_id_quality_avg,
        MAX(kwh_minus_voltage) OVER (PARTITION BY site_id, quality_or_unknown ORDER BY kwh_minus_voltage DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_minus_voltage_max_3_prior,
        ROUND(CUME_DIST() OVER (ORDER BY kwh_minus_voltage NULLS LAST), 3) AS kwh_minus_voltage_cume_dist,
        SIZE(COLLECT_SET(quality_or_unknown) OVER w_site_id_quality) AS distinct_quality_or_unknown_in_site_id_quality,
        LEAD(read_at, 2) OVER (ORDER BY read_at, id) AS next2_read_at,
        MAX(kwh_minus_voltage) OVER (ORDER BY kwh_minus_voltage NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS kwh_minus_voltage_max_2,
        LEAD(kwh, 3) OVER (PARTITION BY site_id, quality_or_unknown ORDER BY kwh_minus_voltage DESC, id) AS next3_kwh,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id, quality_or_unknown ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        MAX(kwh) OVER (PARTITION BY quality_or_unknown, quality) AS quality_or_unknown_quality_max_kwh,
        COUNT(*) OVER (PARTITION BY quality_or_unknown, quality) AS rows_in_quality_or_unknown_quality,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality_or_unknown ORDER BY kwh_minus_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage
    FROM recent
    WINDOW w_all_by_read_at AS (ORDER BY read_at, id), w_site_id_quality AS (PARTITION BY site_id, quality)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh_minus_voltage,
        quality_or_unknown,
        read_at_tercile,
        site_id_quality_sum_kwh,
        kwh_minus_voltage_vs_site_id_quality_avg,
        kwh_minus_voltage_max_3_prior,
        kwh_minus_voltage_cume_dist,
        distinct_quality_or_unknown_in_site_id_quality,
        next2_read_at,
        kwh_minus_voltage_max_2,
        first_known_read_at,
        rows_in_quality_or_unknown_quality,
        first_known_voltage,
        DATEDIFF(DATE '2025-12-31', next2_read_at) AS days_since_next2_read_at,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM windowed
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    kwh_minus_voltage,
    read_at_tercile,
    site_id_quality_sum_kwh,
    kwh_minus_voltage_vs_site_id_quality_avg,
    kwh_minus_voltage_max_3_prior,
    kwh_minus_voltage_cume_dist,
    first_known_read_at,
    rows_in_quality_or_unknown_quality,
    first_known_voltage,
    quality_group,
    INITCAP(TRIM(quality_or_unknown)) AS quality_or_unknown_clean
FROM tagged
ORDER BY quality_group DESC
