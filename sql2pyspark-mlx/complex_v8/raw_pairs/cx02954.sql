WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage < 233.1
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.kwh_per_voltage,
        s.quality_slug,
        si.zone AS site_zone,
        si.site_name
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN kwh_per_voltage >= 8.0 THEN 'HIGH' WHEN kwh_per_voltage >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_per_voltage_tier,
        ROUND(AVG(kwh_per_voltage), 2) AS avg_kwh_per_voltage,
        ARRAY_SORT(COLLECT_SET(site_zone)) AS site_zone_list,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad,
        MAX(kwh_per_voltage) AS max_kwh_per_voltage
    FROM combined_details_site
    GROUP BY 1
    HAVING SUM(kwh) > 24.5
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(max_kwh_per_voltage) OVER (ORDER BY avg_kwh_per_voltage DESC, kwh_per_voltage_tier ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS max_kwh_per_voltage_avg_6_prior,
        MIN(n_quality_bad) OVER (PARTITION BY kwh_per_voltage_tier) AS kwh_per_voltage_tier_min_n_quality_bad
    FROM totals
)
SELECT
    kwh_per_voltage_tier,
    avg_kwh_per_voltage,
    n_quality_bad,
    max_kwh_per_voltage,
    n_quality_bad - avg_kwh_per_voltage AS n_quality_bad_minus_avg_kwh_per_voltage
FROM windowed
WHERE kwh_per_voltage_tier IN ('LOW', 'HIGH')
