WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ABS(kwh - 57.3) AS kwh_distance,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM recent
),
rollup_base AS (
    -- Summarise per group
    SELECT
        read_at_year,
        MAX(read_at) AS last_read_at,
        SUM(least_voltage_kwh) AS total_least_voltage_kwh,
        COUNT(*) AS row_count,
        MAX(kwh) AS max_kwh,
        SUM(kwh_distance) AS total_kwh_distance
    FROM tagged
    WHERE site_id IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) >= 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT read_at_year, metric, amount
FROM (
    SELECT read_at_year, total_least_voltage_kwh, total_kwh_distance
    FROM rollup_base
) src
UNPIVOT (
    amount FOR metric IN (total_least_voltage_kwh, total_kwh_distance)
) unp
