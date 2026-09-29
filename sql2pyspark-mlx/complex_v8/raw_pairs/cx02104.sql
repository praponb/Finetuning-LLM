WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE site_id >= 2
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM scoped
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        read_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio,
        MIN(read_at_year) AS first_read_at_year,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        MAX(read_at_year) AS last_read_at_year
    FROM latest
    GROUP BY site_id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 229.9
        )
)
SELECT
    site_id,
    voltage_to_kwh_ratio,
    first_read_at_year,
    last_read_at_year,
    ROUND(median_kwh / NULLIF(voltage_to_kwh_ratio, 0), 3) AS median_kwh_per_voltage_to_kwh_ratio,
    ROUND(voltage_to_kwh_ratio * 0.9, 2) AS voltage_to_kwh_ratio_adj
FROM with_activity
WHERE TO_DATE(last_read_at_year) < DATE '2025-01-21'
ORDER BY site_id DESC NULLS FIRST, voltage_to_kwh_ratio DESC NULLS FIRST
