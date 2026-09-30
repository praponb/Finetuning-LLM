WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(voltage * 0.25, 2) AS voltage_adj,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage_adj,
        quality_or_unknown,
        COALESCE(voltage, 0.0) AS voltage_filled,
        COALESCE(kwh, 0.0) AS kwh_filled,
        CASE WHEN quality_or_unknown IN ('UNKNOWN', 'BAD') THEN 'group_a' WHEN quality_or_unknown = 'EST' THEN 'group_b' ELSE 'other' END AS quality_or_unknown_group
    FROM filtered
    WHERE voltage > 216.8
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, kwh_filled, voltage, voltage_filled
    FROM annotated
) src
UNPIVOT (
    value FOR metric IN (kwh_filled AS m_kwh_filled, voltage AS voltage_v, voltage_filled AS voltage_filled_v)
) unp
