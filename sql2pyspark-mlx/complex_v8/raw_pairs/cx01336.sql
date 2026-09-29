WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh >= 45.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        SUM(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_sum_1_prior
    FROM base
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE kwh > (SELECT AVG(kwh) FROM with_windows)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        voltage - kwh AS voltage_minus_kwh,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM filtered
)
SELECT id, metric_name, amount
FROM (
    SELECT id, voltage_minus_kwh, kwh, voltage
    FROM enriched
) src
UNPIVOT (
    amount FOR metric_name IN (voltage_minus_kwh AS m_voltage_minus_kwh, kwh AS kwh_v, voltage AS m_voltage)
) unp
ORDER BY id, metric_name
