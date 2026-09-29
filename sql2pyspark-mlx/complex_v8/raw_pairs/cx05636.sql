WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(voltage, 0.0) AS voltage_filled,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage < 241.8
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        quality_or_unknown,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage,
        ROUND(SUM(voltage_filled), 2) AS total_voltage_filled,
        MIN(read_at) AS first_read_at,
        MAX(read_at) AS last_read_at,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(voltage_filled), 2) AS stddev_voltage_filled
    FROM prepared_readings
    WHERE voltage_filled >= 213.5
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Add calculated columns
    SELECT
        site_id,
        quality_or_unknown,
        read_at_month,
        stddev_voltage,
        last_read_at,
        n_rows,
        stddev_voltage_filled,
        LOWER(REGEXP_REPLACE(TRIM(quality_or_unknown), ' ', '_')) AS quality_or_unknown_slug,
        CASE WHEN quality_or_unknown IN ('UNKNOWN', 'OK') THEN 'group_a' WHEN quality_or_unknown = 'EST' THEN 'group_b' ELSE 'other' END AS quality_or_unknown_group,
        COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
    FROM rollup_base
)
SELECT site_id, quality_or_unknown, metric_name, amount
FROM (
    SELECT site_id, quality_or_unknown, stddev_voltage, stddev_voltage_filled
    FROM scored
) src
UNPIVOT (
    amount FOR metric_name IN (stddev_voltage AS m_stddev_voltage, stddev_voltage_filled AS m_stddev_voltage_filled)
) unp
