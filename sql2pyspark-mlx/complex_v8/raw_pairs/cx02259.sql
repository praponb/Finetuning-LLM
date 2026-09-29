WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'BAD')
),
grouped AS (
    -- Group-level totals
    SELECT
        CASE WHEN voltage >= 232.4 THEN 'large' WHEN voltage >= 218.4 THEN 'medium' ELSE 'small' END AS voltage_tier,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM base
    GROUP BY CASE WHEN voltage >= 232.4 THEN 'large' WHEN voltage >= 218.4 THEN 'medium' ELSE 'small' END
),
derived AS (
    -- Label and score rows
    SELECT
        voltage_tier,
        distinct_qualitys,
        stddev_voltage,
        quality_list,
        COALESCE(voltage_tier, 'UNKNOWN') AS voltage_tier_or_unknown,
        INITCAP(TRIM(voltage_tier)) AS voltage_tier_clean
    FROM grouped
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(voltage_tier), ' ', '_')) AS voltage_tier_slug,
        distinct_qualitys + stddev_voltage AS distinct_qualitys_plus_stddev_voltage
    FROM derived
    WHERE stddev_voltage > 85.8
),
latest_per_key AS (
    -- Keep the latest row per voltage_tier_or_unknown
    SELECT
        voltage_tier,
        distinct_qualitys,
        stddev_voltage,
        quality_list,
        voltage_tier_or_unknown,
        voltage_tier_clean,
        voltage_tier_slug,
        distinct_qualitys_plus_stddev_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY voltage_tier_or_unknown ORDER BY stddev_voltage DESC, voltage_tier DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        stddev_voltage - distinct_qualitys AS stddev_voltage_minus_distinct_qualitys
    FROM latest_per_key
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE stddev_voltage > (SELECT AVG(stddev_voltage) FROM enriched)
)
SELECT
    voltage_tier,
    distinct_qualitys,
    stddev_voltage,
    quality_list,
    distinct_qualitys_plus_stddev_voltage,
    stddev_voltage_minus_distinct_qualitys,
    ABS(distinct_qualitys - 5) AS distinct_qualitys_distance
FROM screened
ORDER BY stddev_voltage_minus_distinct_qualitys DESC
