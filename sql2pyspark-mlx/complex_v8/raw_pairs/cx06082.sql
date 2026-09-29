WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE kwh > 15.9
      AND quality <> 'EST'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM filtered_readings
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at_day) - UNIX_TIMESTAMP(LAG(read_at_day) OVER w_site_id_by_kwh)) / 60, 1) AS minutes_since_prev_read_at_day,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, TO_DATE(read_at))) AS distinct_site_id_in_quality_read_at_to_date,
        NTH_VALUE(voltage_bucket, 2) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_bucket,
        COUNT(*) OVER (PARTITION BY quality, TO_DATE(read_at)) AS rows_in_quality_read_at_to_date
    FROM tagged
    WINDOW w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, kwh, kwh_minus_voltage, minutes_since_prev_read_at_day, second_kwh
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (kwh AS kwh_v, kwh_minus_voltage AS m_kwh_minus_voltage, minutes_since_prev_read_at_day AS m_minutes_since_prev_read_at_day, second_kwh AS m_second_kwh)
) unp
ORDER BY id, measure
