WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage_2
    FROM meter_readings
    WHERE kwh < 110.5
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        greatest_kwh_voltage,
        kwh_bucket,
        greatest_kwh_voltage_2,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM valid_readings
    WHERE greatest_kwh_voltage < 196.0
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh_bucket,
        quality_slug,
        quality_or_unknown,
        SUM(greatest_kwh_voltage) OVER (ORDER BY greatest_kwh_voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_kwh_voltage,
        ROW_NUMBER() OVER w_all_by_greatest_kwh_voltage_2 AS seq_no,
        kwh_bucket - LAG(kwh_bucket) OVER (ORDER BY greatest_kwh_voltage, id) AS kwh_bucket_change,
        ROUND(AVG(kwh) OVER (PARTITION BY quality), 2) AS quality_avg_kwh,
        NTILE(4) OVER (ORDER BY greatest_kwh_voltage, id) AS greatest_kwh_voltage_quartile,
        FIRST_VALUE(kwh_bucket) IGNORE NULLS OVER (ORDER BY greatest_kwh_voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_bucket
    FROM tagged
    WINDOW w_all_by_greatest_kwh_voltage_2 AS (ORDER BY greatest_kwh_voltage, id)
)
SELECT id, metric_name, amount
FROM (
    SELECT id, quality_avg_kwh, voltage, kwh
    FROM with_windows
) src
UNPIVOT (
    amount FOR metric_name IN (quality_avg_kwh AS quality_avg_kwh_v, voltage AS m_voltage, kwh AS m_kwh)
) unp
