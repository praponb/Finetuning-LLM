WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE quality <> 'EST'
      AND site_id >= 5
),
labeled AS (
    -- Label and score rows
    SELECT *, LEAST(least_kwh_voltage, kwh) AS least_least_kwh_voltage_kwh
    FROM recent_readings
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        SUM(CASE WHEN quality = 'BAD' THEN 1 ELSE 0 END) AS n_quality_bad,
        MAX(voltage) AS max_voltage
    FROM labeled
    GROUP BY 1
    HAVING SUM(least_kwh_voltage) > 85.8
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(max_voltage) IGNORE NULLS OVER (PARTITION BY n_quality_bad ORDER BY n_quality_bad, read_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_voltage
    FROM per_group
)
SELECT read_at_ym, n_quality_bad, first_known_max_voltage
FROM with_trends
WHERE read_at_ym IS NOT NULL
ORDER BY first_known_max_voltage
