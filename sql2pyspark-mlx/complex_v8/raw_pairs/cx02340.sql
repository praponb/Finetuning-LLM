WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE kwh <= 113.1
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.voltage > 223.1
        )
),
labeled AS (
    -- Derive helper columns
    SELECT *, ABS(least_kwh_voltage - 171.5) AS least_kwh_voltage_distance
    FROM qualified
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(kwh, 0.0) AS kwh_filled,
        LEAST(least_kwh_voltage, kwh) AS least_least_kwh_voltage_kwh
    FROM labeled
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        quality,
        site_id,
        ROUND(STDDEV(kwh_filled), 2) AS stddev_kwh_filled,
        COUNT(*) AS row_count,
        MAX(read_at) AS last_read_at,
        ROUND(SUM(least_kwh_voltage), 2) AS total_least_kwh_voltage
    FROM scored
    GROUP BY DATE_TRUNC('MONTH', read_at), quality, site_id
    HAVING COUNT(*) >= 2
),
annotated AS (
    -- Add calculated columns
    SELECT
        read_at_month,
        quality,
        site_id,
        stddev_kwh_filled,
        last_read_at,
        FLOOR(total_least_kwh_voltage / 250) * 250 AS total_least_kwh_voltage_bucket
    FROM rollup_base
    WHERE row_count < 10
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_read_at) - UNIX_TIMESTAMP(read_at_month)) / 3600, 1) AS hours_read_at_month_to_last_read_at,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM annotated
)
-- Final projection
SELECT
    read_at_month,
    quality,
    stddev_kwh_filled,
    total_least_kwh_voltage_bucket,
    quality_prefix
FROM enriched
ORDER BY read_at_month DESC
