WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE quality IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage
    FROM recent
),
grouped AS (
    -- Group-level totals
    SELECT
        site_id,
        quality,
        MAX(read_at) AS last_read_at,
        MAX(running_kwh) AS max_running_kwh,
        ROUND(STDDEV(last_known_voltage), 2) AS stddev_last_known_voltage
    FROM ranked
    GROUP BY site_id, quality
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.site_id,
        s.quality,
        s.last_read_at,
        s.max_running_kwh,
        s.stddev_last_known_voltage,
        si.site_name,
        si.tariff AS site_tariff
    FROM grouped s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT site_id, quality, metric, value
FROM (
    SELECT site_id, quality, stddev_last_known_voltage, max_running_kwh
    FROM with_parent
) src
UNPIVOT (
    value FOR metric IN (stddev_last_known_voltage AS stddev_last_known_voltage_v, max_running_kwh AS m_max_running_kwh)
) unp
